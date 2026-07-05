#pragma once
#include <string>
#include <vector>

class IStorageBackend
{
public:
  virtual ~IStorageBackend() = default;

  // Saves a chunk's data to the underlying storage
  virtual bool SaveChunk(const std::string &hash, const std::vector<char> &data) = 0;

  // Loads a chunk's data from the underlying storage
  virtual std::vector<char> LoadChunk(const std::string &hash) = 0;

  // Deletes a chunk from the underlying storage
  virtual bool DeleteChunk(const std::string &hash) = 0;

  // Checks if a chunk exists in the underlying storage
  virtual bool ChunkExists(const std::string &hash) = 0;
};