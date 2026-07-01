#pragma once
#include "IMetadataRepository.hpp"
#include <pqxx/pqxx>
#include <vector>
#include <string>
#include <sstream>
#include <iostream>
#include <exception>

class PostgresRepository : public IMetadataRepository
{
private:
  std::string connection_string;

public:
  PostgresRepository(const std::string &conn_str) : connection_string(conn_str) {}

  // --- CONNECTIVITY TEST ---
  std::string GetDatabaseVersion() override
  {
    pqxx::connection conn(connection_string);
    pqxx::work txn(conn);
    pqxx::result res = txn.exec("SELECT version();");
    txn.commit();
    return res[0][0].as<std::string>();
  }

  // --- DEDUPLICATION CHECK & LEASE RENEWAL ---
  std::vector<std::string> FilterMissingChunks(const std::vector<std::string> &client_hashes) override
  {
    std::vector<std::string> missing_hashes;
    pqxx::connection conn(connection_string);
    pqxx::work txn(conn);

    for (const auto &hash : client_hashes)
    {
      // Fetch the actual ref_count to categorize the chunk's lifecycle state
      pqxx::result res = txn.exec_params(
          "SELECT ref_count FROM chunks WHERE chunk_hash = $1", hash);

      if (res.empty())
      {
        // STATE A: Chunk is completely missing from the database.
        // We pre-register the chunk with ref_count = 0 (Two-Phase Commit).
        missing_hashes.push_back(hash);
        txn.exec_params(
            "INSERT INTO chunks (chunk_hash, nodes, ref_count, created_at) VALUES ($1, $2, 0, CURRENT_TIMESTAMP) "
            "ON CONFLICT (chunk_hash) DO NOTHING",
            hash,
            "{worker_1:9001, worker_2:9002}" // Mapped cluster nodes (can be dynamically assigned via heartbeats later)
        );
      }
      else
      {
        int ref_count = res[0][0].as<int>();

        if (ref_count == -1)
        {
          // STATE B: TOMBSTONE LOCK. The Garbage Collector is physically deleting this right now.
          // Throwing an exception forces the MasterServer to return HTTP 423 Locked, telling the client to retry in 1s.
          throw std::runtime_error("CHUNK_LOCKED");
        }
        else
        {
          // LEASE RENEWAL: Reset the 24-hour GC countdown timer.
          // This protects the chunk from the Garbage Collector during active uploads or client-side pause/resumes.
          txn.exec_params(
              "UPDATE chunks SET created_at = CURRENT_TIMESTAMP WHERE chunk_hash = $1",
              hash);

          if (ref_count == 0)
          {
            // STATE C: Aborted/Uncommitted upload. It exists, but might be incomplete on the Worker.
            // Do NOT deduplicate against it. Force the client to upload/overwrite it.
            missing_hashes.push_back(hash);
          }
          // STATE D: ref_count > 0. Healthy, committed chunk. Skip upload (Perfect deduplication).
        }
      }
    }
    txn.commit();
    return missing_hashes;
  }

  // --- TWO-PHASE COMMIT ---
  void CommitFile(const std::string &filename, long long file_size, const std::vector<std::string> &chunk_hashes) override
  {
    pqxx::connection conn(connection_string);
    pqxx::work txn(conn);

    // 1. Convert C++ vector to PostgreSQL native array format: e.g., {hash1,hash2}
    std::string pg_array = "{";
    for (size_t i = 0; i < chunk_hashes.size(); ++i)
    {
      pg_array += chunk_hashes[i];
      if (i < chunk_hashes.size() - 1)
        pg_array += ",";
    }
    pg_array += "}";

    // 2. Insert the finalized file record (Hardcoding user_id = 1 for MVP)
    txn.exec_params(
        "INSERT INTO files (user_id, filename, size, chunks) VALUES (1, $1, $2, $3)",
        filename,
        file_size,
        pg_array);

    // 3. Batch update: transition all chunks in this file from pending (0) to active (1+)
    for (const auto &hash : chunk_hashes)
    {
      txn.exec_params(
          "UPDATE chunks SET ref_count = ref_count + 1 WHERE chunk_hash = $1",
          hash);
    }

    txn.commit();
  }

  // --- COOPERATIVE ORPHANED CHUNKS MANAGEMENT ---
  std::vector<OrphanedChunk> GetOrphanedChunks(int expiry_hours, int limit) override
  {
    std::vector<OrphanedChunk> orphans;
    try
    {
      pqxx::connection conn(connection_string);
      pqxx::work txn(conn);

      // ATOMIC LOCK & PULL (Tombstone transition):
      // Moves chunks from ref_count = 0 to -1, skipping any currently locked by other Masters.
      // Uses the Partial Index (idx_chunks_orphans_gc) for sub-millisecond query performance.
      std::string query =
          "UPDATE chunks SET ref_count = -1 "
          "WHERE chunk_hash IN ("
          "  SELECT chunk_hash FROM chunks "
          "  WHERE ref_count = 0 "
          "    AND created_at < NOW() - INTERVAL '" +
          std::to_string(expiry_hours) + " hours' "
                                         "  FOR UPDATE SKIP LOCKED "
                                         "  LIMIT " +
          std::to_string(limit) + " "
                                  ") RETURNING chunk_hash, nodes;";

      pqxx::result res = txn.exec(query);

      for (const auto &row : res)
      {
        OrphanedChunk orphan;
        orphan.hash = row[0].as<std::string>();

        // Parse PostgreSQL array string e.g., "{worker_1:9001, worker_2:9002}"
        std::string array_str = row[1].as<std::string>();
        if (array_str.size() > 2)
        {
          array_str = array_str.substr(1, array_str.size() - 2); // Strip '{' and '}'

          std::stringstream ss(array_str);
          std::string node_str;

          // Split by comma
          while (std::getline(ss, node_str, ','))
          {
            // Split by colon to separate Host and Port
            size_t colon_pos = node_str.find(':');
            if (colon_pos != std::string::npos)
            {
              WorkerLocation loc;
              loc.host = node_str.substr(0, colon_pos);
              loc.port = std::stoi(node_str.substr(colon_pos + 1));

              orphan.locations.push_back(loc);
            }
          }
        }
        orphans.push_back(orphan);
      }

      txn.commit(); // Committing permanently registers the -1 Tombstone lock
    }
    catch (const std::exception &e)
    {
      std::cerr << "[DB ERROR] GetOrphanedChunks failed: " << e.what() << "\n";
    }
    return orphans;
  }

  // --- SAFETY GATED DELETION ---
  bool DeleteChunkRecordsBulk(const std::vector<std::string> &hashes) override
  {
    if (hashes.empty())
      return true;

    try
    {
      pqxx::connection conn(connection_string);
      pqxx::work txn(conn);

      // Build the query: DELETE FROM chunks WHERE ref_count = -1 AND chunk_hash IN ($1, $2, ...)
      std::string query = "DELETE FROM chunks WHERE ref_count = -1 AND chunk_hash IN (";
      for (size_t i = 0; i < hashes.size(); ++i)
      {
        query += "'" + txn.esc(hashes[i]) + "'"; // Securely escape strings
        if (i < hashes.size() - 1)
          query += ",";
      }
      query += ");";

      pqxx::result res = txn.exec(query);
      txn.commit();

      std::cout << "[GC] Bulk purged " << res.affected_rows() << " records from PostgreSQL.\n";
      return true;
    }
    catch (const std::exception &e)
    {
      std::cerr << "[DB ERROR] Bulk delete failed: " << e.what() << "\n";
    }
    return false;
  }

  // --- FAILOVER RECOVERY ---
  void RevertTombstone(const std::string &hash) override
  {
    try
    {
      pqxx::connection conn(connection_string);
      pqxx::work txn(conn);

      // RECOVERY STEP: If Worker deletion fails (e.g. node offline), revert -1 back to 0
      // so that the Garbage Collector can safely try again during its next sweep.
      txn.exec_params(
          "UPDATE chunks SET ref_count = 0 "
          "WHERE chunk_hash = $1 AND ref_count = -1;",
          hash);

      txn.commit();
      std::cout << "[GC RECOVERY] Reverted tombstone back to pending for chunk: " << hash << "\n";
    }
    catch (const std::exception &e)
    {
      std::cerr << "[DB ERROR] RevertTombstone failed for " << hash << ": " << e.what() << "\n";
    }
  }
};