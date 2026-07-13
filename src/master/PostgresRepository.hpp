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

  std::string GetDatabaseVersion() override
  {
    pqxx::connection conn(connection_string);
    pqxx::work txn(conn);
    pqxx::result res = txn.exec("SELECT version();");
    txn.commit();
    return res[0][0].as<std::string>();
  }

  bool CreateUser(const std::string &email, const std::string &password_hash, const std::string &salt) override
  {
    try
    {
      pqxx::connection conn(connection_string);
      pqxx::work txn(conn);

      txn.exec_params(
          "INSERT INTO users (email, password_hash, salt) VALUES ($1, $2, $3);",
          email, password_hash, salt);

      txn.commit();
      return true;
    }
    catch (const std::exception &e)
    {
      std::cerr << "[DB ERROR] Failed to create user: " << e.what() << "\n";
      return false; // Typically a unique constraint violation (email exists)
    }
  }

  UserRecord GetUserByEmail(const std::string &email) override
  {
    UserRecord record;
    try
    {
      pqxx::connection conn(connection_string);
      pqxx::work txn(conn);

      pqxx::result res = txn.exec_params(
          "SELECT id, password_hash, salt FROM users WHERE email = $1;", email);

      if (!res.empty())
      {
        record.found = true;
        record.id = res[0][0].as<int>();
        record.password_hash = res[0][1].as<std::string>();
        record.salt = res[0][2].as<std::string>();
      }
      txn.commit();
    }
    catch (const std::exception &e)
    {
      std::cerr << "[DB ERROR] GetUserByEmail failed: " << e.what() << "\n";
      throw; // Propagate exception to prevent swallowing infrastructure failures
    }
    return record;
  }

  void RegisterWorker(const std::string &internal_host, int internal_port,
                      const std::string &advertised_host, int advertised_port,
                      long long free_space) override
  {
    try
    {
      pqxx::connection conn(connection_string);
      pqxx::work txn(conn);

      txn.exec_params(
          "INSERT INTO worker_nodes (internal_host, internal_port, advertised_host, advertised_port, free_space, last_heartbeat) "
          "VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP) "
          "ON CONFLICT (internal_host, internal_port) "
          "DO UPDATE SET "
          "    advertised_host = EXCLUDED.advertised_host, "
          "    advertised_port = EXCLUDED.advertised_port, "
          "    free_space = EXCLUDED.free_space, "
          "    last_heartbeat = CURRENT_TIMESTAMP, "
          "    status = 'ACTIVE';",
          internal_host, internal_port, advertised_host, advertised_port, free_space);

      txn.commit();
    }
    catch (const std::exception &e)
    {
      std::cerr << "[DB ERROR] Worker registration failed: " << e.what() << "\n";
      throw;
    }
  }

  std::vector<std::string> FilterMissingChunks(const std::vector<std::string> &client_hashes) override
  {
    std::vector<std::string> missing_hashes;
    pqxx::connection conn(connection_string);
    pqxx::work txn(conn);

    for (const auto &hash : client_hashes)
    {
      pqxx::result res = txn.exec_params(
          "SELECT ref_count FROM chunks WHERE chunk_hash = $1", hash);

      if (res.empty())
      {
        missing_hashes.push_back(hash);
        txn.exec_params(
            "INSERT INTO chunks (chunk_hash, nodes, ref_count, created_at) VALUES ($1, $2, 0, CURRENT_TIMESTAMP) "
            "ON CONFLICT (chunk_hash) DO NOTHING",
            hash,
            "{worker_1:9001, worker_2:9002}"); // Hardcoded ports to be replaced dynamically by routing logic
      }
      else
      {
        int ref_count = res[0][0].as<int>();

        if (ref_count == -1)
        {
          throw std::runtime_error("CHUNK_LOCKED");
        }
        else
        {
          txn.exec_params(
              "UPDATE chunks SET created_at = CURRENT_TIMESTAMP WHERE chunk_hash = $1", hash);

          if (ref_count == 0)
          {
            missing_hashes.push_back(hash);
          }
        }
      }
    }
    txn.commit();
    return missing_hashes;
  }

  void CommitFile(const std::string &filename, long long file_size, const std::vector<std::string> &chunk_hashes) override
  {
    pqxx::connection conn(connection_string);
    pqxx::work txn(conn);

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
        filename, file_size, pg_array);

    for (const auto &hash : chunk_hashes)
    {
      txn.exec_params(
          "UPDATE chunks SET ref_count = ref_count + 1 WHERE chunk_hash = $1", hash);
    }

    txn.commit();
  }

  std::vector<OrphanedChunk> GetOrphanedChunks(int expiry_hours, int limit) override
  {
    std::vector<OrphanedChunk> orphans;
    try
    {
      pqxx::connection conn(connection_string);
      pqxx::work txn(conn);

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

        std::string array_str = row[1].as<std::string>();
        if (array_str.size() > 2)
        {
          array_str = array_str.substr(1, array_str.size() - 2);
          std::stringstream ss(array_str);
          std::string node_str;

          while (std::getline(ss, node_str, ','))
          {
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

      txn.commit();
    }
    catch (const std::exception &e)
    {
      std::cerr << "[DB ERROR] GetOrphanedChunks failed: " << e.what() << "\n";
    }
    return orphans;
  }

  bool DeleteChunkRecordsBulk(const std::vector<std::string> &hashes) override
  {
    if (hashes.empty())
      return true;

    try
    {
      pqxx::connection conn(connection_string);
      pqxx::work txn(conn);

      std::string query = "DELETE FROM chunks WHERE ref_count = -1 AND chunk_hash IN (";
      for (size_t i = 0; i < hashes.size(); ++i)
      {
        query += "'" + txn.esc(hashes[i]) + "'";
        if (i < hashes.size() - 1)
          query += ",";
      }
      query += ");";

      pqxx::result res = txn.exec(query);
      txn.commit();

      return true;
    }
    catch (const std::exception &e)
    {
      std::cerr << "[DB ERROR] Bulk delete failed: " << e.what() << "\n";
    }
    return false;
  }

  void RevertTombstone(const std::string &hash) override
  {
    try
    {
      pqxx::connection conn(connection_string);
      pqxx::work txn(conn);

      txn.exec_params(
          "UPDATE chunks SET ref_count = 0 WHERE chunk_hash = $1 AND ref_count = -1;", hash);

      txn.commit();
    }
    catch (const std::exception &e)
    {
      std::cerr << "[DB ERROR] RevertTombstone failed for " << hash << ": " << e.what() << "\n";
    }
  }
};