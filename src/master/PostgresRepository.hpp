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

  // --- AUTHENTICATION ---

  // Inserts a new user record. Uses parameterized queries to prevent SQL injection.
  // Returns false if a unique constraint violation occurs (e.g., email already exists).
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
      return false;
    }
  }

  // Retrieves user credentials based on email for session token generation.
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

  // --- WORKER NODE DISCOVERY ---

  // Executes an atomic UPSERT to register or update a Worker Node.
  // Refreshes the last_heartbeat timestamp and updates capacity metrics.
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
        // STATE A: Chunk is missing. Pre-register with ref_count = 0 (Two-Phase Commit).
        missing_hashes.push_back(hash);
        txn.exec_params(
            "INSERT INTO chunks (chunk_hash, nodes, ref_count, created_at) VALUES ($1, $2, 0, CURRENT_TIMESTAMP) "
            "ON CONFLICT (chunk_hash) DO NOTHING",
            hash,
            "{127.0.0.1:9001, 127.0.0.1:9002}"); // To be replaced dynamically by routing logic
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
          // LEASE RENEWAL: Reset the 24-hour GC countdown timer.
          txn.exec_params(
              "UPDATE chunks SET created_at = CURRENT_TIMESTAMP WHERE chunk_hash = $1",
              hash);

          if (ref_count == 0)
          {
            // STATE C: Aborted/Uncommitted upload. Force client to upload/overwrite it.
            missing_hashes.push_back(hash);
          }
          // STATE D: ref_count > 0. Healthy, committed chunk. Skipped safely.
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
    // Also updates the 'created_at' timestamp to ensure that if this chunk ever drops back
    // to a reference count of 0, the 24-hour deletion grace period starts from that deletion time.
    for (const auto &hash : chunk_hashes)
    {
      txn.exec_params(
          "UPDATE chunks SET ref_count = ref_count + 1, created_at = CURRENT_TIMESTAMP WHERE chunk_hash = $1",
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

      // Atomic UPDATE + RETURNING using SKIP LOCKED.
      // Modifies target rows from 0 to -1, preventing concurrent transactions from selecting them.
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

  // --- SAFETY GATED DELETION ---
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

      // Reverts -1 to 0 and resets the 'created_at' lease timestamp.
      // This grants the failed/offline Worker Node a fresh 24-hour lease window to recover,
      // preventing the Garbage Collector from immediately re-sweeping it on the next loop.
      txn.exec_params(
          "UPDATE chunks SET ref_count = 0, created_at = CURRENT_TIMESTAMP "
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