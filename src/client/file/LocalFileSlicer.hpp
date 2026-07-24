#pragma once
#include "IFileSlicer.hpp"
#include "../crypto/ICryptoEngine.hpp"
#include <memory>
#include <fstream>

// Implements a stateful, two-pass file evaluation mechanism.
// Separates the CPU-bound hashing (Pass 1) from the I/O-bound network streaming (Pass 2)
// to strictly limit the RAM footprint during massive file uploads.
class LocalFileSlicer : public IFileSlicer
{
private:
  std::shared_ptr<ICryptoEngine> crypto_engine_; // Injected cryptographic dependency
  std::ifstream file_stream;                     // Persistent handle for Pass 2 random access
  size_t current_chunk_size;                     // Defines the strict byte boundary for physical reads

public:
  explicit LocalFileSlicer(std::shared_ptr<ICryptoEngine> crypto_engine);
  ~LocalFileSlicer();

  // Pass 1: Cryptographic Evaluation
  // Utilized by DSEClient::UploadFile to build the "Master Blueprint" of the file.
  // Executes a multithreaded, memory-bounded sweep of the physical file to generate an
  // ordered list of SHA-256 hashes. These hashes are sent to the Control Plane for deduplication.
  std::vector<std::string> ComputeHashes(const std::string &filepath, size_t chunk_size) override;

  // Pass 2 Initialization: Stream State
  // Utilized by ParallelTransferManager::Upload before dispatching network threads.
  // Opens the file descriptor to allow for targeted, random-access disk reads.
  void OpenStream(const std::string &filepath, size_t chunk_size) override;

  // Pass 2 Execution: Targeted Data Retrieval
  // Utilized by ParallelTransferManager::Upload inside the network dispatch loop.
  // Computes the absolute byte offset mathematically (index * chunk_size) and uses
  // direct SSD random access (seekg) to extract only the specific chunks requested by the cluster.
  std::vector<char> ReadChunk(int index, size_t chunk_size) override;

  // Teardown: Resource Release
  // Utilized by the SlicerStreamGuard (RAII) in ParallelTransferManager.
  // Closes the physical file descriptor to prevent OS lock leaks upon completion or stack unwinding.
  void CloseStream() override;
};