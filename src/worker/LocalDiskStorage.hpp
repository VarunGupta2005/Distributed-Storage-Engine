#pragma once
#include "IStorageBackend.hpp"
#include "../common/crypto.hpp"
#include <filesystem>
#include <fstream>
#include <vector>
#include <iostream>
#include <system_error>
#include <stdexcept>
#include <mutex>
#include <cstdlib>

class LocalDiskStorage : public IStorageBackend
{
  const unsigned char *node_key;

private:
  std::filesystem::path storage_dir;
  std::mutex storage_mutex;      // Protects disk space queries and in-flight reservations
  long long in_flight_bytes = 0; // Tracks bytes currently being written

  // Checks and reserves disk space atomically to prevent overallocation.
  bool ReserveSpace(size_t chunk_size_bytes)
  {
    std::lock_guard<std::mutex> lock(storage_mutex);

    std::error_code ec;
    auto space_info = std::filesystem::space(storage_dir, ec);
    if (ec)
    {
      std::cerr << "[I/O ERROR] Failed to query disk space: " << ec.message() << "\n";
      return false;
    }

    long long actual_safe_space = static_cast<long long>(space_info.available) - in_flight_bytes;
    long long needed_space = static_cast<long long>(chunk_size_bytes) + (50LL * 1024 * 1024); // 50MB buffer

    if (actual_safe_space < needed_space)
    {
      return false;
    }

    in_flight_bytes += chunk_size_bytes;
    return true;
  }

public:
  // Initializes the storage directory. Throws on failure to prevent operations without valid storage.
  LocalDiskStorage(const std::string &dir, const unsigned char *key) : storage_dir(dir), node_key(key)
  {
    std::error_code ec;
    std::filesystem::create_directories(storage_dir, ec);

    if (ec)
    {
      throw std::runtime_error("Failed to initialize storage directory: " + ec.message());
    }
  }

  // Reserves space, writes the chunk data, and releases the reservation.
  bool SaveChunk(const std::string &hash, const std::vector<char> &data) override
  {
    std::vector<char> encrypted_data;

    // Encrypt the raw client payload in memory using AES-256-CBC
    try
    {
      encrypted_data = EncryptBytes(data, node_key);
    }
    catch (const std::exception &e)
    {
      std::cerr << "[I/O ERROR] Encryption failed for " << hash << ": " << e.what() << "\n";
      return false; // Safely fail-fast before making any database or disk reservations [16]
    }

    // Check and reserve the exact physical write size in memory before starting disk I/O
    if (!ReserveSpace(encrypted_data.size()))
    {
      std::cerr << "[I/O WARNING] Refused write for " << hash << ": Insufficient disk space.\n";
      return false;
    }

    std::filesystem::path filepath = storage_dir / (hash + ".bin");
    std::ofstream file(filepath, std::ios::binary);

    if (!file.is_open())
    {
      // Release the exact reservation under the lock on file open failure
      std::lock_guard<std::mutex> lock(storage_mutex);
      in_flight_bytes -= encrypted_data.size();
      return false;
    }

    file.write(encrypted_data.data(), encrypted_data.size());

    // Flush the stream to the OS cache before evaluating success.
    file.flush();
    bool success = file.good();

    // Release the exact reservation under the lock
    {
      std::lock_guard<std::mutex> lock(storage_mutex);
      in_flight_bytes -= encrypted_data.size();
    }

    return success;
  }

  // Reads a chunk from the disk into a memory buffer.
  std::vector<char> LoadChunk(const std::string &hash) override
  {
    std::filesystem::path filepath = storage_dir / (hash + ".bin");

    std::ifstream file(filepath, std::ios::binary | std::ios::ate);
    if (!file.is_open())
      return {};

    std::streamsize size = file.tellg();

    if (size < 0)
      return {};

    file.seekg(0, std::ios::beg);

    std::vector<char> buffer(size);
    if (file.read(buffer.data(), size))
    {
      try
      {
        return DecryptBytes(buffer, node_key);
      }
      catch (const std::exception &e)
      {
        std::cerr << "[I/O ERROR] Decryption failed for " << hash << ": " << e.what() << "\n";
        return {};
      }
    }
    return {};
  }

  // Removes the chunk from the filesystem. Returns true if deleted or already absent.
  bool DeleteChunk(const std::string &hash) override
  {
    std::filesystem::path filepath = storage_dir / (hash + ".bin");
    std::error_code ec;

    std::filesystem::remove(filepath, ec);

    // Check for OS-level errors (e.g., permission denied).
    // If ec is clear, the file was successfully removed or didn't exist in the first place.
    if (ec)
    {
      std::cerr << "[I/O ERROR] Failed to delete " << hash << ": " << ec.message() << "\n";
      return false;
    }

    return true;
  }

  // Validates the existence of a chunk binary on the disk.
  bool ChunkExists(const std::string &hash) override
  {
    std::filesystem::path filepath = storage_dir / (hash + ".bin");
    std::error_code ec;

    bool exists = std::filesystem::exists(filepath, ec);

    if (ec)
    {
      std::cerr << "[I/O ERROR] Chunk existence check failed: " << ec.message() << "\n";
      return false;
    }
    return exists;
  }
};