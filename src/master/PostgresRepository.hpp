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

  const int num_buckets;

  // Resolves worker_ids to JSON objects containing network coordinates
  ChunkRoutingPlan ParseNodePlanFromJson(const std::string &hash, const std::string &json_nodes_data)
  {
    ChunkRoutingPlan plan;
    plan.hash = hash;

    if (json_nodes_data.empty() || json_nodes_data == "null")
      return plan;

    auto nodes_json = nlohmann::json::parse(json_nodes_data);
    for (const auto &item : nodes_json)
    {
      NodeInfo info;
      info.advertised_host = item["advertised_host"].get<std::string>();
      info.advertised_port = item["advertised_port"].get<int>();
      plan.assigned_nodes.push_back(info);
    }
    return plan;
  }

  inline int CalculateBucketId(const std::string &sha256_hex, int num_buckets)
  {
    if (sha256_hex.length() < 8)
      return 0;
    try
    {
      std::string prefix = sha256_hex.substr(0, 8);
      unsigned int hash_int = std::stoul(prefix, nullptr, 16);
      return hash_int % num_buckets;
    }
    catch (...)
    {
      return 0;
    }
  }

  void VerifyNodesExist(pqxx::work &txn, const std::vector<int> &node_ids)
  {
    if (node_ids.empty())
    {
      throw std::runtime_error("Validation failed: Worker ID list cannot be empty.");
    }

    std::unordered_set<int> unique_ids(node_ids.begin(), node_ids.end());

    std::string pg_array = "{";
    size_t index = 0;
    for (int id : unique_ids)
    {
      pg_array += std::to_string(id);
      if (++index < unique_ids.size())
      {
        pg_array += ",";
      }
    }
    pg_array += "}";

    pqxx::result count_res = txn.exec_params(
        "SELECT COUNT(*) FROM worker_nodes WHERE worker_id = ANY($1::integer[])",
        pg_array);

    int valid_count = count_res[0][0].as<int>();
    if (valid_count != static_cast<int>(unique_ids.size()))
    {
      throw std::runtime_error("Validation failed: One or more provided worker IDs do not exist.");
    }
  }

public:
  PostgresRepository(const std::string &conn_str, const int &buckets) : connection_string(conn_str), num_buckets(buckets)
  {
  }

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
      return false;
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
      throw;
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
          "INSERT INTO worker_nodes (internal_host, internal_port, advertised_host, advertised_port, free_space, last_heartbeat, status) "
          "VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP, 'ACTIVE') "
          "ON CONFLICT (internal_host, internal_port) "
          "DO UPDATE SET "
          "    advertised_host = EXCLUDED.advertised_host, "
          "    advertised_port = EXCLUDED.advertised_port, "
          "    free_space = EXCLUDED.free_space, "
          "    last_heartbeat = CURRENT_TIMESTAMP, "
          "    status = CASE "
          "                 WHEN worker_nodes.status = 'READ_ONLY' THEN 'READ_ONLY' "
          "                 ELSE 'ACTIVE' "
          "             END;",
          internal_host, internal_port, advertised_host, advertised_port, free_space);

      txn.commit();
    }
    catch (const std::exception &e)
    {
      std::cerr << "[DB ERROR] Worker registration failed: " << e.what() << "\n";
      throw;
    }
  }

  std::vector<ChunkRoutingPlan> FilterMissingChunks(const std::vector<std::string> &client_hashes) override
  {
    std::vector<ChunkRoutingPlan> missing_plans;
    pqxx::connection conn(connection_string);
    pqxx::work txn(conn);

    for (const auto &hash : client_hashes)
    {
      pqxx::result res = txn.exec_params(
          "SELECT ref_count, nodes, "
          "(SELECT json_agg(json_build_object("
          "   'internal_coord', internal_host || ':' || internal_port, "
          "   'advertised_host', advertised_host, "
          "   'advertised_port', advertised_port"
          ")) FROM worker_nodes WHERE worker_id = ANY(chunks.nodes)) AS node_data "
          "FROM chunks WHERE chunk_hash = $1",
          hash);

      if (res.empty())
      {
        int bucket_id = CalculateBucketId(hash, num_buckets);

        pqxx::result routing_res = txn.exec_params(
            "SELECT c.active_nodes, "
            "(SELECT json_agg(json_build_object("
            "   'internal_coord', internal_host || ':' || internal_port, "
            "   'advertised_host', advertised_host, "
            "   'advertised_port', advertised_port"
            ")) FROM worker_nodes WHERE worker_id = ANY(c.active_nodes)) AS node_data "
            "FROM shard_map s "
            "JOIN clusters c ON s.cluster_id = c.cluster_id "
            "WHERE s.bucket_id = $1",
            bucket_id);

        if (routing_res.empty())
        {
          throw std::runtime_error("Shard map uninitialized for bucket " + std::to_string(bucket_id));
        }

        std::string pg_nodes_array = routing_res[0][0].as<std::string>();
        std::string json_nodes_data = routing_res[0][1].as<std::string>();

        txn.exec_params(
            "INSERT INTO chunks (chunk_hash, nodes, ref_count, created_at) VALUES ($1, $2, 0, CURRENT_TIMESTAMP) "
            "ON CONFLICT (chunk_hash) DO NOTHING",
            hash, pg_nodes_array);

        missing_plans.push_back(ParseNodePlanFromJson(hash, json_nodes_data));
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
          txn.exec_params("UPDATE chunks SET created_at = CURRENT_TIMESTAMP WHERE chunk_hash = $1", hash);

          if (ref_count == 0)
          {
            std::string json_nodes_data = res[0][2].as<std::string>();
            missing_plans.push_back(ParseNodePlanFromJson(hash, json_nodes_data));
          }
        }
      }
    }
    txn.commit();
    return missing_plans;
  }

  void CommitFile(int user_id, const std::string &filename, long long file_size, const std::vector<std::string> &chunk_hashes) override
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

    // Inserts the finalized file record using the actual validated user_id
    txn.exec_params(
        "INSERT INTO files (user_id, filename, size, chunks) VALUES ($1, $2, $3, $4)",
        user_id,
        filename,
        file_size,
        pg_array);

    for (const auto &hash : chunk_hashes)
    {
      txn.exec_params(
          "UPDATE chunks SET ref_count = ref_count + 1, created_at = CURRENT_TIMESTAMP WHERE chunk_hash = $1",
          hash);
    }

    txn.commit();
  }

  FileDownloadManifest GetDownloadPlan(int user_id, const std::string &filename) override
  {
    pqxx::connection conn(connection_string);
    pqxx::work txn(conn);

    // 1. Fetch file size to confirm existence and multi-tenant authorization
    pqxx::result file_res = txn.exec_params(
        "SELECT size FROM files WHERE filename = $1 AND user_id = $2;",
        filename, user_id);

    if (file_res.empty())
    {
      throw std::runtime_error("File not found or access denied.");
    }

    FileDownloadManifest manifest;
    manifest.file_size = file_res[0][0].as<long long>();

    // 2. Resolve all chunk routing plans in a single ordered query
    pqxx::result chunk_res = txn.exec_params(
        "SELECT u.chunk_hash, "
        "(SELECT json_agg(json_build_object("
        "   'internal_coord', w.internal_host || ':' || w.internal_port, "
        "   'advertised_host', w.advertised_host, "
        "   'advertised_port', w.advertised_port"
        ")) FROM worker_nodes w WHERE w.worker_id = ANY(c.nodes)) AS node_data "
        "FROM files f "
        "CROSS JOIN unnest(f.chunks) WITH ORDINALITY AS u(chunk_hash, ord) "
        "JOIN chunks c ON c.chunk_hash = u.chunk_hash "
        "WHERE f.filename = $1 AND f.user_id = $2 "
        "ORDER BY u.ord",
        filename, user_id);

    for (const auto &row : chunk_res)
    {
      std::string hash = row[0].as<std::string>();
      std::string json_nodes_data = row[1].is_null() ? "[]" : row[1].as<std::string>();
      manifest.plans.push_back(ParseNodePlanFromJson(hash, json_nodes_data));
    }

    txn.commit();
    return manifest;
  }

  std::vector<OrphanedChunk> GetOrphanedChunks(int expiry_hours, int limit) override
  {
    std::vector<OrphanedChunk> orphans;
    try
    {
      pqxx::connection conn(connection_string);
      pqxx::work txn(conn);

      std::string query =
          "WITH updated AS ("
          "  UPDATE chunks SET ref_count = -1 "
          "  WHERE chunk_hash IN ("
          "    SELECT chunk_hash FROM chunks WHERE ref_count = 0 AND created_at < NOW() - INTERVAL '" +
          std::to_string(expiry_hours) + " hours' FOR UPDATE SKIP LOCKED LIMIT " +
          std::to_string(limit) +
          "  ) RETURNING chunk_hash, nodes"
          ") "
          "SELECT u.chunk_hash, "
          "(SELECT json_agg(json_build_object('host', w.internal_host, 'port', w.internal_port)) "
          " FROM worker_nodes w WHERE w.worker_id = ANY(u.nodes)) AS locations "
          "FROM updated u;";

      pqxx::result res = txn.exec(query);

      for (const auto &row : res)
      {
        OrphanedChunk orphan;
        orphan.hash = row[0].as<std::string>();

        std::string json_str = row[1].is_null() ? "[]" : row[1].as<std::string>();
        auto nodes_json = nlohmann::json::parse(json_str);

        for (const auto &item : nodes_json)
        {
          WorkerLocation loc;
          loc.host = item["host"].get<std::string>();
          loc.port = item["port"].get<int>();
          orphan.locations.push_back(loc);
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

      std::cout << "[GC] Bulk purged " << res.affected_rows() << " records from PostgreSQL.\n";
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

  void AddCluster(const std::string &cluster_name, const std::vector<int> &active_node_ids) override
  {
    pqxx::connection conn(connection_string);
    pqxx::work txn(conn);

    VerifyNodesExist(txn, active_node_ids);

    std::string pg_array = "{";
    for (size_t i = 0; i < active_node_ids.size(); ++i)
    {
      pg_array += std::to_string(active_node_ids[i]);
      if (i < active_node_ids.size() - 1)
        pg_array += ",";
    }
    pg_array += "}";

    pqxx::result insert_res = txn.exec_params(
        "INSERT INTO clusters (cluster_name, active_nodes) VALUES ($1, $2) RETURNING cluster_id",
        cluster_name, pg_array);
    int new_cluster_id = insert_res[0][0].as<int>();

    pqxx::result current_clusters = txn.exec(
        "SELECT cluster_id, COUNT(bucket_id) FROM shard_map GROUP BY cluster_id");
    std::cout << "[DEBUG] current_clusters.size() = "
              << current_clusters.size() << "\n";

    std::cout << "[DEBUG] new_cluster_id = "
              << new_cluster_id << "\n";

    std::cout << "[DEBUG] num_buckets = "
              << num_buckets << "\n";

    if (current_clusters.empty())
    {
      std::cout << "[DEBUG] BOOTSTRAPPING SHARD MAP\n";
      pqxx::result result = txn.exec_params(
          "INSERT INTO shard_map (bucket_id, cluster_id) "
          "SELECT g.id, $1 FROM generate_series(0, $2) AS g(id) "
          "ON CONFLICT (bucket_id) DO UPDATE SET cluster_id = EXCLUDED.cluster_id",
          new_cluster_id, num_buckets - 1);
      std::cout << "[DEBUG] Inserted "
                << result.affected_rows()
                << " shard_map rows\n";
    }
    else
    {
      int total_clusters = current_clusters.size() + 1;
      int target_buckets = num_buckets / total_clusters;

      for (const auto &row : current_clusters)
      {
        int existing_cluster_id = row[0].as<int>();
        int current_owned = row[1].as<int>();
        int buckets_to_reassign = current_owned - target_buckets;

        if (buckets_to_reassign > 0)
        {
          txn.exec_params(
              "UPDATE shard_map SET cluster_id = $1 "
              "WHERE bucket_id IN ( "
              "    SELECT bucket_id FROM shard_map WHERE cluster_id = $2 LIMIT $3 "
              ")",
              new_cluster_id, existing_cluster_id, buckets_to_reassign);
        }
      }
    }

    txn.commit();
  }

  void ReplaceCluster(int cluster_id, const std::vector<int> &new_worker_ids) override
  {
    pqxx::connection conn(connection_string);
    pqxx::work txn(conn);

    pqxx::result cluster_check = txn.exec_params(
        "SELECT 1 FROM clusters WHERE cluster_id = $1", cluster_id);
    if (cluster_check.empty())
    {
      throw std::runtime_error("Validation failed: Target cluster_id does not exist.");
    }

    VerifyNodesExist(txn, new_worker_ids);

    txn.exec_params(
        "UPDATE worker_nodes SET status = 'READ_ONLY' "
        "WHERE worker_id IN (SELECT unnest(active_nodes) FROM clusters WHERE cluster_id = $1)",
        cluster_id);

    std::string pg_array = "{";
    for (size_t i = 0; i < new_worker_ids.size(); ++i)
    {
      pg_array += std::to_string(new_worker_ids[i]);
      if (i < new_worker_ids.size() - 1)
        pg_array += ",";
    }
    pg_array += "}";

    txn.exec_params(
        "UPDATE clusters SET active_nodes = $1 WHERE cluster_id = $2",
        pg_array, cluster_id);

    txn.commit();
  }
  bool FileExists(int user_id, const std::string &filename) override
  {
    pqxx::connection conn(connection_string);
    pqxx::work txn(conn);
    pqxx::result res = txn.exec_params(
        "SELECT 1 FROM files WHERE user_id = $1 AND filename = $2;",
        user_id, filename);
    txn.commit();
    return !res.empty();
  }

  bool DeleteFile(int user_id, const std::string &filename) override
  {
    pqxx::connection conn(connection_string);
    pqxx::work txn(conn);

    // 1. Fetch the array of chunk hashes for the target file
    pqxx::result res = txn.exec_params(
        "SELECT chunks FROM files WHERE user_id = $1 AND filename = $2;",
        user_id, filename);

    if (res.empty())
    {
      return false; // File does not exist
    }

    std::string pg_array = res[0][0].as<std::string>();

    // 2. Decrement ref_count for all chunks using native array expansion
    txn.exec_params(
        "UPDATE chunks SET ref_count = ref_count - 1 WHERE chunk_hash = ANY($1::text[]);",
        pg_array);

    // 3. Delete the file record from the database
    txn.exec_params(
        "DELETE FROM files WHERE user_id = $1 AND filename = $2;",
        user_id, filename);

    txn.commit();
    return true;
  }
};
