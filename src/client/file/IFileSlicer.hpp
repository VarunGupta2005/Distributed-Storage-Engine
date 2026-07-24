#pragma once
#include <string>
#include <vector>

class IFileSlicer
{
public:
  virtual ~IFileSlicer() = default;

  virtual std::vector<std::string> ComputeHashes(const std::string &filepath, size_t chunk_size) = 0;
  virtual void OpenStream(const std::string &filepath, size_t chunk_size) = 0;

  // Simplified: Directly returns the binary payload. Struct overhead is eliminated.
  virtual std::vector<char> ReadChunk(int index, size_t chunk_size) = 0;

  virtual void CloseStream() = 0;
};