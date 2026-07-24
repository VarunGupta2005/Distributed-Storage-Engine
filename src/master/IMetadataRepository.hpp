#pragma once
#include <string>
#include <vector>
#include "../common/models.hpp"

struct WorkerLocation
{
  std::string host;
  int port;
};

struct OrphanedChunk
{
  std::string hash;
  std::vector<WorkerLocation> locations;
};

struct UserRecord
{
  bool found = false;
  int id = 0;
  std::string password_hash;
  std::string salt;
};

class IMetadataRepository
{
public:
  virtual ~IMetadataRepository() = default;

  // --- CONNECTIVITY ---
  virtual std::string GetDatabaseVersion() = 0;

  // --- AUTHENTICATION ---
  virtual bool CreateUser(const std::string &email, const std::string &password_hash, const std::string &salt) = 0;
  virtual UserRecord GetUserByEmail(const std::string &email) = 0;

  // --- WORKER NODE DISCOVERY ---
  virtual void RegisterWorker(const std::string &internal_host, int internal_port,
                              const std::string &advertised_host, int advertised_port,
                              long long free_space) = 0;

  // --- DEDUPLICATION & STORAGE ---
  virtual std::vector<ChunkRoutingPlan> FilterMissingChunks(const std::vector<std::string> &client_hashes) = 0;

  // Updated to receive the authorized user_id to enforce multi-tenant schema isolation.
  virtual void CommitFile(int user_id, const std::string &filename, long long file_size, const std::vector<std::string> &chunk_hashes) = 0;

  // Retrieves the ordered list of chunks and their replicas for authorized file downloads.
  virtual FileDownloadManifest GetDownloadPlan(int user_id, const std::string &filename) = 0;

  // Checks if a file with the given name exists for the authenticated user
  virtual bool FileExists(int user_id, const std::string &filename) = 0;

  // Deletes the file record and decrements ref_counts for its chunks in a single transaction
  virtual bool DeleteFile(int user_id, const std::string &filename) = 0;

  // --- GARBAGE COLLECTION ---
  virtual std::vector<OrphanedChunk> GetOrphanedChunks(int expiry_hours, int limit) = 0;
  virtual bool DeleteChunkRecordsBulk(const std::vector<std::string> &hashes) = 0;
  virtual void RevertTombstone(const std::string &hash) = 0;

  // --- CLUSTER SCALING (ADMIN) ---
  virtual void AddCluster(const std::string &cluster_name, const std::vector<int> &active_node_ids) = 0;
  virtual void ReplaceCluster(int cluster_id, const std::vector<int> &new_worker_ids) = 0;
};