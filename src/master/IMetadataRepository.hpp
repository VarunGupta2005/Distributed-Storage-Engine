#pragma once
#include <string>
#include <vector>

class IMetadataRepository
{
public:
  virtual ~IMetadataRepository() = default;

  // 1. Returns the version of the database
  virtual std::string GetDatabaseVersion() = 0;

  // 2. Returns a list of hashes that DO NOT exist in the database yet
  virtual std::vector<std::string> FilterMissingChunks(const std::vector<std::string> &client_hashes) = 0;

  // 3. Creates the file record and increments the ref_count for these chunks
  virtual void CommitFile(const std::string &filename, long long file_size, const std::vector<std::string> &chunk_hashes) = 0;
};