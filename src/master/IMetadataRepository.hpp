#pragma once
#include <string>
#include <vector>

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
// Represents the physical destination for a chunk upload
struct NodeInfo
{
  std::string internal_coord;  // Format: "internal_host:port" (used for DB registry)
  std::string advertised_host; // Format: "192.168.1.40" (used by the Client)
  int advertised_port;
};

// Maps a missing chunk to its dynamically assigned Replica Set
struct MissingChunkPlan
{
  std::string hash;
  std::vector<NodeInfo> assigned_nodes;
};

class IMetadataRepository
{
public:
  virtual ~IMetadataRepository() = default;

  // --- CONNECTIVITY ---
  // Returns the version of the database to verify connectivity.
  virtual std::string GetDatabaseVersion() = 0;

  // --- AUTHENTICATION ---
  // Inserts a new user into the database. Returns false if the email already exists.
  virtual bool CreateUser(const std::string &email, const std::string &password_hash, const std::string &salt) = 0;

  // Retrieves user credentials by email for authentication verification.
  virtual UserRecord GetUserByEmail(const std::string &email) = 0;

  // --- WORKER NODE DISCOVERY ---
  // Registers or updates a Worker Node via its heartbeat payload using the Dual-Identity routing model.
  virtual void RegisterWorker(const std::string &internal_host, int internal_port,
                              const std::string &advertised_host, int advertised_port,
                              long long free_space) = 0;

  // --- DEDUPLICATION & STORAGE ---
  // UPDATED: Now returns a comprehensive routing plan linking missing hashes to active storage nodes.
  virtual std::vector<MissingChunkPlan> FilterMissingChunks(const std::vector<std::string> &client_hashes) = 0;
  // Finalizes the two-phase commit by creating the file record and incrementing ref_counts.
  virtual void CommitFile(const std::string &filename, long long file_size, const std::vector<std::string> &chunk_hashes) = 0;

  // --- GARBAGE COLLECTION ---
  // Returns a list of orphaned chunks (ref_count = 0) older than the TTL, locking them as tombstones (-1).
  virtual std::vector<OrphanedChunk> GetOrphanedChunks(int expiry_hours, int limit) = 0;

  // Deletes metadata records for chunks that were successfully purged from physical worker nodes.
  virtual bool DeleteChunkRecordsBulk(const std::vector<std::string> &hashes) = 0;

  // Reverts a tombstoned chunk (-1) back to pending (0) if the physical deletion fails.
  virtual void RevertTombstone(const std::string &hash) = 0;

  // --- CLUSTER SCALING (ADMIN) ---
  // Adds a new cluster to the shard_map with its active write nodes.
  virtual void AddCluster(const std::string &cluster_name, const std::vector<int> &active_node_ids) = 0;
  // Replaces the filled cluster nodes with new empty ones
  virtual void ReplaceCluster(int cluster_id, const std::vector<int> &new_worker_ids) = 0;
};