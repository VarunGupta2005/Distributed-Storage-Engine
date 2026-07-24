#include "ParallelTransferManager.hpp"
#include "../../common/httplib.h"
#include "../../common/Semaphore.hpp"
#include <fstream>
#include <mutex>
#include <iostream>
#include <cstring>
#include <filesystem>
#include <deque>
#include <future>
#include <thread>
#include <chrono>

ParallelTransferManager::ParallelTransferManager(std::shared_ptr<ICryptoEngine> crypto_engine)
    : crypto_engine_(std::move(crypto_engine)) {}

struct SlicerStreamGuard
{
  IFileSlicer *slicer_ptr;
  explicit SlicerStreamGuard(IFileSlicer *s) : slicer_ptr(s) {}
  ~SlicerStreamGuard() { slicer_ptr->CloseStream(); }
};

static bool UploadChunkToWorker(const std::string &host, int port, const std::string &hash, const std::vector<char> &data)
{
  for (int attempt = 0; attempt < 3; ++attempt)
  {
    try
    {
      httplib::Client client(host, port);
      client.set_connection_timeout(2, 0);
      auto res = client.Post("/chunk?hash=" + hash, data.data(), data.size(), "application/octet-stream");

      if (res && res->status == 200)
      {
        return true;
      }
      else if (res)
      {
        std::cerr << "[WARNING] Worker " << host << ":" << port << " rejected upload. HTTP " << res->status << "\n";
      }
    }
    catch (const std::exception &e)
    {
      std::cerr << "[WARNING] Upload attempt " << attempt + 1 << " to " << host << ":" << port << " failed: " << e.what() << "\n";
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(500 * (1 << attempt)));
  }
  std::cerr << "[ERROR] Max retries exhausted for Worker " << host << ":" << port << "\n";
  return false;
}

static void DownloadChunkFromReplica(const std::vector<NodeInfo> &nodes, const std::string &hash, std::vector<char> &chunk_buffer, ICryptoEngine *crypto_engine)
{
  size_t expected_size = chunk_buffer.size();
  for (const auto &node : nodes)
  {
    try
    {
      httplib::Client client(node.advertised_host, node.advertised_port);
      client.set_connection_timeout(2, 0);
      size_t bytes_written = 0;

      auto res = client.Get("/chunk?hash=" + hash, [&](const char *data, size_t data_length)
                            {
                if (bytes_written + data_length > expected_size) return false;
                std::memcpy(chunk_buffer.data() + bytes_written, data, data_length);
                bytes_written += data_length;
                return true; });

      if (!res || res->status != 200 || bytes_written != expected_size)
      {
        std::cerr << "[FAILOVER] Worker " << node.advertised_host << ":" << node.advertised_port << " returned incomplete data. Trying next.\n";
        continue;
      }

      if (crypto_engine->ComputeHash(chunk_buffer) != hash)
      {
        std::cerr << "[FAILOVER] Hash mismatch from Worker " << node.advertised_host << ":" << node.advertised_port << ". Trying next.\n";
        continue;
      }

      return;
    }
    catch (const std::exception &e)
    {
      std::cerr << "[FAILOVER] Worker " << node.advertised_host << ":" << node.advertised_port << " connection failed: " << e.what() << "\n";
    }
  }
  throw std::runtime_error("All replicas failed or returned corrupted data for chunk: " + hash);
}

// Upload method implementation remains structurally identical to our last refinement
bool ParallelTransferManager::Upload(IFileSlicer *slicer, ILocalStateManager *state_manager,
                                     const std::string &filepath, size_t chunk_size,
                                     const std::vector<std::pair<int, ChunkRoutingPlan>> &pending_tasks,
                                     std::atomic<TransferState> *state)
{
  httplib::ThreadPool pool(16);
  std::deque<std::future<bool>> futures;
  Semaphore in_flight_limiter(16);

  std::mutex log_mutex;
  bool all_succeeded = true;
  std::vector<int> pending_disk_writes;

  slicer->OpenStream(filepath, chunk_size);
  SlicerStreamGuard guard(slicer);

  for (const auto &task : pending_tasks)
  {
    if (state->load() != TransferState::RUNNING)
      break;

    while (!futures.empty() && futures.front().wait_for(std::chrono::seconds(0)) == std::future_status::ready)
    {
      if (!futures.front().get())
        all_succeeded = false;
      futures.pop_front();
    }
    if (!all_succeeded)
      break;

    int index = task.first;
    const ChunkRoutingPlan &plan = task.second;

    in_flight_limiter.acquire();
    std::vector<char> raw_payload = slicer->ReadChunk(index, chunk_size);
    auto chunk_payload = std::make_shared<std::vector<char>>(std::move(raw_payload));
    auto permit = std::make_shared<InFlightPermit>(in_flight_limiter);

    for (const auto &node : plan.assigned_nodes)
    {
      auto promise = std::make_shared<std::promise<bool>>();
      futures.push_back(promise->get_future());

      pool.enqueue([promise, node, hash = plan.hash, index, chunk_payload, permit,
                    &log_mutex, &pending_disk_writes, filepath, state_manager]() mutable
                   {
                try {
                    bool success = UploadChunkToWorker(node.advertised_host, node.advertised_port, hash, *chunk_payload);

                    if (success) {
                        std::lock_guard<std::mutex> lock(log_mutex);
                        pending_disk_writes.push_back(index);
                        if (pending_disk_writes.size() >= 64) {
                            state_manager->AppendCompletedIndices(filepath, pending_disk_writes);
                            pending_disk_writes.clear();
                        }
                    }
                    
                    // Release memory and resolve promise safely
                    chunk_payload.reset();
                    permit.reset();
                    promise->set_value(success);
                    
                } catch (...) {
                    // Safety net: Guarantee the main thread never deadlocks!
                    chunk_payload.reset();
                    permit.reset();
                    promise->set_exception(std::current_exception());
                } });
    }
  }

  for (auto &fut : futures)
  {
    if (!fut.get())
      all_succeeded = false;
  }
  pool.shutdown();
  if (!pending_disk_writes.empty())
  {
    state_manager->AppendCompletedIndices(filepath, pending_disk_writes);
    pending_disk_writes.clear();
  }

  return all_succeeded && state->load() == TransferState::RUNNING;
}

// DownloadToFile method implementation remains structurally identical to our last refinement
bool ParallelTransferManager::DownloadToFile(const std::vector<DownloadChunkMeta> &ordered_chunks,
                                             const std::vector<ChunkRoutingPlan> &upload_plans,
                                             const std::string &output_path,
                                             std::atomic<TransferState> *state)
{
  std::unordered_map<std::string, std::vector<NodeInfo>> routing_map;
  for (const auto &plan : upload_plans)
  {
    routing_map[plan.hash] = plan.assigned_nodes;
  }

  size_t total_file_size = 0;
  for (const auto &chunk : ordered_chunks)
  {
    total_file_size += chunk.size;
  }

  std::error_code fs_error;
  bool is_resume = std::filesystem::exists(output_path) && std::filesystem::file_size(output_path, fs_error) == total_file_size;

  if (!is_resume)
  {
    std::ofstream init_file(output_path, std::ios::out | std::ios::binary);
    if (!init_file)
      throw std::runtime_error("Failed to allocate file.");
    if (total_file_size > 0)
    {
      init_file.seekp(total_file_size - 1);
      init_file.write("", 1);
    }
  }

  std::fstream shared_file(output_path, std::ios::in | std::ios::out | std::ios::binary);
  if (!shared_file)
    throw std::runtime_error("Failed to open file stream.");

  std::vector<bool> chunk_completed(ordered_chunks.size(), false);

  if (is_resume)
  {
    std::cout << "[DEBUG] Executing local cryptographic verification on partial file...\n";
    size_t offset = 0;
    for (size_t i = 0; i < ordered_chunks.size(); ++i)
    {
      if (state->load() != TransferState::RUNNING)
        break;

      std::vector<char> buffer(ordered_chunks[i].size);
      shared_file.seekg(offset, std::ios::beg);
      shared_file.read(buffer.data(), buffer.size());

      if (crypto_engine_->ComputeHash(buffer) == ordered_chunks[i].hash)
      {
        chunk_completed[i] = true;
      }
      offset += ordered_chunks[i].size;
    }
  }

  std::mutex file_mutex;
  httplib::ThreadPool pool(16);
  std::vector<std::future<void>> futures;
  size_t current_offset = 0;

  for (size_t i = 0; i < ordered_chunks.size(); ++i)
  {
    const auto &chunk = ordered_chunks[i];

    if (state->load() != TransferState::RUNNING)
      break;
    if (chunk_completed[i])
    {
      current_offset += chunk.size;
      continue;
    }

    auto it = routing_map.find(chunk.hash);
    if (it == routing_map.end())
      throw std::runtime_error("Missing network route for hash: " + chunk.hash);

    auto nodes = it->second;
    auto promise = std::make_shared<std::promise<void>>();
    futures.push_back(promise->get_future());

    size_t chunk_offset = current_offset;
    ICryptoEngine *crypto_engine = crypto_engine_.get();

    pool.enqueue([promise, nodes, chunk, chunk_offset, &shared_file, &file_mutex, crypto_engine]()
                 {
            try {
                std::vector<char> chunk_buffer(chunk.size);
                DownloadChunkFromReplica(nodes, chunk.hash, chunk_buffer, crypto_engine);

                {
                    std::lock_guard<std::mutex> lock(file_mutex);
                    shared_file.seekp(chunk_offset, std::ios::beg);
                    shared_file.write(chunk_buffer.data(), chunk.size);
                }
                promise->set_value();
            } catch (...) {
                promise->set_exception(std::current_exception());
            } });

    current_offset += chunk.size;
  }

  for (auto &fut : futures)
  {
    fut.get();
  }

  pool.shutdown();
  return state->load() == TransferState::RUNNING;
}