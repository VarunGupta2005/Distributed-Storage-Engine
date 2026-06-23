#pragma once
#include "IMetadataRepository.hpp"
#include <pqxx/pqxx>
#include <vector>
#include <string>

class PostgresRepository : public IMetadataRepository
{
private:
  std::string connection_string;

public:
  PostgresRepository(const std::string &conn_str) : connection_string(conn_str) {}

  std::string GetDatabaseVersion() override
  {
    pqxx::connection conn(connection_string);
    pqxx::work txn(conn);
    pqxx::result res = txn.exec("SELECT version();");
    txn.commit();
    return res[0][0].as<std::string>();
  }

  // --- THE DEDUPLICATION CHECK ---
  std::vector<std::string> FilterMissingChunks(const std::vector<std::string> &client_hashes) override
  {
    std::vector<std::string> missing_hashes;
    pqxx::connection conn(connection_string);
    pqxx::work txn(conn);

    for (const auto &hash : client_hashes)
    {
      // Query to see if the chunk exists in our chunks table
      pqxx::result res = txn.exec_params(
          "SELECT 1 FROM chunks WHERE chunk_hash = $1",
          hash);

      // If the query returns 0 rows, the chunk is missing!
      if (res.empty())
      {
        missing_hashes.push_back(hash);

        // Pre-register the chunk with ref_count = 0 so we reserve it
        txn.exec_params(
            "INSERT INTO chunks (chunk_hash, nodes, ref_count) VALUES ($1, $2, 0) "
            "ON CONFLICT (chunk_hash) DO NOTHING",
            hash,
            "{9001, 9002}" // Temporary hardcoded ports for MVP testing
        );
      }
    }
    txn.commit();
    return missing_hashes;
  }

  // --- THE TWO-PHASE COMMIT ---
  void CommitFile(const std::string &filename, long long file_size, const std::vector<std::string> &chunk_hashes) override
  {
    pqxx::connection conn(connection_string);
    pqxx::work txn(conn);

    // 1. Insert the file record (Hardcoding user_id = 1 for MVP)
    // Convert C++ vector to PostgreSQL native array format: {hash1,hash2}
    std::string pg_array = "{";
    for (size_t i = 0; i < chunk_hashes.size(); ++i)
    {
      pg_array += chunk_hashes[i];
      if (i < chunk_hashes.size() - 1)
        pg_array += ",";
    }
    pg_array += "}";

    txn.exec_params(
        "INSERT INTO files (user_id, filename, size, chunks) VALUES (1, $1, $2, $3)",
        filename,
        file_size,
        pg_array);

    // 2. Increment the reference counts for all chunks in this file
    for (const auto &hash : chunk_hashes)
    {
      txn.exec_params(
          "UPDATE chunks SET ref_count = ref_count + 1 WHERE chunk_hash = $1",
          hash);
    }

    txn.commit();
  }
};