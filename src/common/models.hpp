#pragma once
#include <string>
#include <vector>
#include <unordered_set>

// Lifecycle state enumerator for active data plane transfers.
enum class TransferState
{
  RUNNING,
  PAUSED,
  CANCELED
};

// Represents the network routing coordinates for a physical storage node.
struct NodeInfo
{
  std::string advertised_host;
  int advertised_port;
};

// Maps a requested data block to its allocated replica set.
struct ChunkRoutingPlan
{
  std::string hash;
  std::vector<NodeInfo> assigned_nodes;
};

// Specifies strict byte boundaries for direct memory access during downloads.
struct DownloadChunkMeta
{
  std::string hash;
  size_t size;
};

// Unified payload returned by the Control Plane download initialization.
struct FileDownloadManifest
{
  long long file_size;
  std::vector<ChunkRoutingPlan> plans;
};

// Ephemeral state tracker for uncommitted upload progress.
struct UploadManifest
{
  std::string filepath;
  long long total_size = 0;
  long long temp_modified_timestamp = 0;
  long long session_timestamp = 0;
  std::vector<std::string> chunk_hashes;
  std::vector<bool> completed_chunks;
  int successfully_uploaded_count = 0;
};