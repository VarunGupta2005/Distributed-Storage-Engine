#include "DSEClient.hpp"

// Concrete subsystem includes required for unique_ptr instantiation
#include "crypto/OpenSSLCryptoEngine.hpp"
#include "file/LocalFileSlicer.hpp"
#include "network/MasterHttpClient.hpp"
#include "network/ParallelTransferManager.hpp"

#include <chrono>
#include <filesystem>
#include <iostream>
#include <iomanip>
#include <stdexcept>
#include <utility>
#include <algorithm>
#include <cstdint>

// Monotonic clock selection for reliable interval timing
using Clock = std::chrono::steady_clock;
using Duration = std::chrono::duration<double>;

DSEClient::DSEClient(const std::string &master_url, std::unique_ptr<ILocalStateManager> state_mgr)
    : state_manager_(std::move(state_mgr))
{
  crypto_engine_ = std::make_shared<OpenSSLCryptoEngine>();
  slicer_ = std::make_unique<LocalFileSlicer>(crypto_engine_);
  metadata_client_ = std::make_unique<MasterHttpClient>(master_url);
  transfer_manager_ = std::make_unique<ParallelTransferManager>(crypto_engine_);

  std::string saved_token = state_manager_->LoadAuthToken();
  if (!saved_token.empty())
  {
    metadata_client_->SetAuthToken(saved_token);
  }
}

bool DSEClient::Signup(const std::string &email, const std::string &password)
{
  std::cout << "[DEBUG] Attempting signup for " << email << "...\n";
  if (metadata_client_->CreateUser(email, password))
  {
    std::cout << "[SUCCESS] Signup complete.\n";
    return true;
  }
  return false;
}

bool DSEClient::Login(const std::string &email, const std::string &password)
{
  std::cout << "[DEBUG] Attempting login for " << email << "...\n";
  std::string token = metadata_client_->Login(email, password);
  if (!token.empty())
  {
    metadata_client_->SetAuthToken(token);
    state_manager_->SaveAuthToken(token);
    std::cout << "[SUCCESS] Authentication successful. Token saved to configuration.\n";
    return true;
  }
  std::cerr << "[ERROR] Authentication failed.\n";
  return false;
}

bool DSEClient::UploadFile(const std::string &filepath, std::atomic<TransferState> *state)
{
  const size_t CHUNK_SIZE = 1024 * 1024;
  std::error_code fs_error;

  // --- PRE-FLIGHT EXISTING FILE CHECK (Non-Blocking) ---
  try
  {
    std::string filename = std::filesystem::path(filepath).filename().string();
    if (metadata_client_->FileExists(filename))
    {
      std::cerr << "\n[ERROR] A file named '" << filename << "' already exists in your account.\n"
                << "Please delete the existing file using the 'delete' command before uploading a new one with the same name.\n\n";
      return false;
    }
  }
  catch (const std::exception &e)
  {
    std::cerr << "[ERROR] Could not check file existence: " << e.what() << "\n";
    return false;
  }

  auto total_start_time = Clock::now();

  std::cout << "[DEBUG] Inspecting local file...\n";
  auto initial_size = std::filesystem::file_size(filepath, fs_error);
  if (fs_error)
  {
    std::cerr << "[ERROR] Cannot access file size: " << fs_error.message() << "\n";
    return false;
  }

  auto actual_modified_time = std::filesystem::last_write_time(filepath, fs_error).time_since_epoch().count();
  if (fs_error)
  {
    std::cerr << "[ERROR] Cannot access file timestamp: " << fs_error.message() << "\n";
    return false;
  }

  auto current_sys_time = std::chrono::duration_cast<std::chrono::seconds>(
                              std::chrono::system_clock::now().time_since_epoch())
                              .count();

  UploadManifest manifest;
  bool is_resume_session = false;

  // 1. Resume Detection & Garbage Collection Verification
  if (state_manager_->HasActiveUpload(filepath))
  {
    std::cout << "[DEBUG] Checking existing local state...\n";
    manifest = state_manager_->LoadUploadManifest(filepath);

    bool file_modified = (manifest.total_size != static_cast<long long>(initial_size) ||
                          manifest.temp_modified_timestamp != actual_modified_time);

    bool gc_expired = (current_sys_time - manifest.session_timestamp) > 82800;

    if (file_modified || gc_expired)
    {
      if (gc_expired)
        std::cerr << "[WARNING] Upload paused for over 23h. Server GC likely purged partial data. Restarting.\n";
      else
        std::cerr << "[WARNING] File modified since last attempt. Discarding state and restarting.\n";

      state_manager_->DeleteUploadManifest(filepath);
      manifest = UploadManifest();
    }
    else
    {
      std::cout << "[INFO] Resuming upload from local state manifest...\n";
      is_resume_session = true;
    }
  }

  double hash_duration_sec = 0.0;
  bool was_hashed_in_session = false;

  // 2. Pass 1: Initialization & Blueprint Generation
  if (manifest.chunk_hashes.empty())
  {
    std::cout << "[DEBUG] Phase 1: Computing cryptographic hashes...\n";
    auto hash_start_time = Clock::now();

    manifest.filepath = filepath;
    manifest.total_size = initial_size;
    manifest.temp_modified_timestamp = actual_modified_time;
    manifest.session_timestamp = current_sys_time;

    try
    {
      manifest.chunk_hashes = slicer_->ComputeHashes(filepath, CHUNK_SIZE);
    }
    catch (const std::exception &e)
    {
      std::cerr << "[ERROR] Hashing failed: " << e.what() << "\n";
      return false;
    }

    hash_duration_sec = std::chrono::duration<double>(Clock::now() - hash_start_time).count();
    was_hashed_in_session = true;

    manifest.completed_chunks.assign(manifest.chunk_hashes.size(), false);
    state_manager_->SaveUploadManifest(manifest);
  }

  // 3. Control Plane Negotiation
  std::cout << "[DEBUG] Phase 2: Negotiating chunk routing with Master Node...\n";
  auto init_start_time = Clock::now();
  std::vector<ChunkRoutingPlan> upload_plans;

  try
  {
    upload_plans = metadata_client_->InitializeUpload(manifest.chunk_hashes);
  }
  catch (const std::exception &e)
  {
    std::cerr << "[ERROR] Control Plane Communication Failed: " << e.what() << "\n";
    return false;
  }
  double init_duration_sec = std::chrono::duration<double>(Clock::now() - init_start_time).count();

  // 4. O(N) Two-Pointer Correlation & Bitset Filter
  std::vector<std::pair<int, ChunkRoutingPlan>> pending_tasks;
  size_t plan_idx = 0;

  for (size_t i = 0; i < manifest.chunk_hashes.size() && plan_idx < upload_plans.size(); ++i)
  {
    if (manifest.chunk_hashes[i] == upload_plans[plan_idx].hash)
    {
      if (!manifest.completed_chunks[i])
      {
        pending_tasks.push_back({static_cast<int>(i), upload_plans[plan_idx]});
      }
      plan_idx++;
    }
  }

  // 5. Pre-Transfer TOCTOU Check
  auto current_size = std::filesystem::file_size(filepath, fs_error);
  auto current_write_time = std::filesystem::last_write_time(filepath, fs_error).time_since_epoch().count();
  if (fs_error || current_size != initial_size || current_write_time != actual_modified_time)
  {
    std::cerr << "[ERROR] File was modified mid-upload. Aborting.\n";
    return false;
  }

  double transfer_duration_sec = 0.0;
  bool transfer_executed = false;

  // 6. Pass 2: Data Plane Transfer
  if (!pending_tasks.empty())
  {
    std::cout << "[DEBUG] Phase 3: Transmitting " << pending_tasks.size() << " missing chunks to Data Plane...\n";
    auto transfer_start_time = Clock::now();

    bool transfer_success = transfer_manager_->Upload(
        slicer_.get(), state_manager_.get(), filepath, CHUNK_SIZE,
        pending_tasks, state);

    transfer_duration_sec = std::chrono::duration<double>(Clock::now() - transfer_start_time).count();
    transfer_executed = true;

    if (!transfer_success)
    {
      std::cerr << "[ERROR] Data transmission failed or was paused.\n";
      return false;
    }
  }
  else
  {
    std::cout << "[INFO] All chunks already exist on cluster (100% Deduplication).\n";
  }

  // 7. Post-Transfer TOCTOU Check
  auto final_size = std::filesystem::file_size(filepath, fs_error);
  auto final_write_time = std::filesystem::last_write_time(filepath, fs_error).time_since_epoch().count();
  if (fs_error || final_size != initial_size || final_write_time != actual_modified_time)
  {
    std::cerr << "[ERROR] File modified during physical upload. Aborting.\n";
    return false;
  }

  // 8. Final Commit
  std::cout << "[DEBUG] Phase 4: Committing metadata schema mapping to database...\n";
  auto commit_start_time = Clock::now();
  std::string filename = std::filesystem::path(filepath).filename().string();

  bool commit_success = metadata_client_->CommitFile(filename, initial_size, manifest.chunk_hashes);
  double commit_duration_sec = std::chrono::duration<double>(Clock::now() - commit_start_time).count();

  if (commit_success)
  {
    state_manager_->DeleteUploadManifest(filepath);
    double total_duration_sec = std::chrono::duration<double>(Clock::now() - total_start_time).count();

    // --- ACCURATE TELEMETRY & BENCHMARK REPORTING ---
    double size_mb = static_cast<double>(initial_size) / (1024.0 * 1024.0);

    // Calculate exact transmitted bytes accounting for tail chunk boundaries
    uint64_t transferred_bytes = 0;
    for (const auto &[index, plan] : pending_tasks)
    {
      uint64_t offset = static_cast<uint64_t>(index) * CHUNK_SIZE;
      if (offset < initial_size)
      {
        transferred_bytes += std::min<uint64_t>(CHUNK_SIZE, initial_size - offset);
      }
    }
    double transferred_mb = static_cast<double>(transferred_bytes) / (1024.0 * 1024.0);

    double dedup_ratio = manifest.chunk_hashes.empty()
                             ? 0.0
                             : (1.0 - (static_cast<double>(upload_plans.size()) / static_cast<double>(manifest.chunk_hashes.size()))) * 100.0;

    std::cout << "\n"
              << "====================================================\n"
              << "            UPLOAD PERFORMANCE TELEMETRY            \n"
              << "====================================================\n"
              << std::fixed << std::setprecision(3)
              << "File Name             : " << filename << "\n"
              << "File Size             : " << size_mb << " MB (" << initial_size << " bytes)\n"
              << "Total Chunks          : " << manifest.chunk_hashes.size() << "\n"
              << "Session Type          : " << (is_resume_session ? "Resumed Session" : "New Upload") << "\n"
              << "Chunks Transmitted    : " << pending_tasks.size() << " (Global Deduplication: " << dedup_ratio << "%)\n"
              << "Transferred Volume    : " << transferred_mb << " MB (" << transferred_bytes << " bytes)\n"
              << "----------------------------------------------------\n";

    if (was_hashed_in_session && hash_duration_sec > 0.000001)
    {
      double hash_speed = size_mb / hash_duration_sec;
      std::cout << "Phase 1: Hashing       : " << hash_duration_sec << " s (" << hash_speed << " MB/s)\n";
    }
    else
    {
      std::cout << "Phase 1: Hashing       : Skipped (Loaded from local state)\n";
    }

    std::cout << "Phase 2: Metadata Init : " << init_duration_sec << " s\n";

    if (transfer_executed && transfer_duration_sec > 0.000001)
    {
      double data_plane_mb_s = transferred_mb / transfer_duration_sec;
      double data_plane_mbps = data_plane_mb_s * 8.0;
      std::cout << "Phase 3: Data Transfer : " << transfer_duration_sec << " s (" << data_plane_mb_s << " MB/s | " << data_plane_mbps << " Mbps)\n";
    }
    else
    {
      std::cout << "Phase 3: Data Transfer : " << transfer_duration_sec << " s (Zero network bytes transmitted)\n";
    }

    std::cout << "Phase 4: Database Commit: " << commit_duration_sec << " s\n"
              << "----------------------------------------------------\n"
              << "Total Execution Time  : " << total_duration_sec << " s\n"
              << "End-to-End Throughput : " << (size_mb / total_duration_sec) << " MB/s (" << ((size_mb / total_duration_sec) * 8.0) << " Mbps)\n"
              << "====================================================\n\n";

    return true;
  }

  std::cerr << "[ERROR] Failed to commit file to the Master Node.\n";
  return false;
}

bool DSEClient::DownloadFile(const std::string &filename, const std::string &output_path, std::atomic<TransferState> *state)
{
  auto total_start_time = Clock::now();

  std::cout << "[DEBUG] Fetching download blueprint from Master Node...\n";
  auto plan_start_time = Clock::now();

  FileDownloadManifest manifest;
  try
  {
    manifest = metadata_client_->GetDownloadPlan(filename);
  }
  catch (const std::exception &e)
  {
    std::cerr << "[ERROR] Failed to fetch layout metadata: " << e.what() << "\n";
    return false;
  }
  double plan_duration_sec = std::chrono::duration<double>(Clock::now() - plan_start_time).count();

  const size_t CHUNK_SIZE = 1024 * 1024;
  std::vector<DownloadChunkMeta> ordered_chunks;
  long long bytes_remaining = manifest.file_size;

  for (const auto &plan : manifest.plans)
  {
    DownloadChunkMeta meta;
    meta.hash = plan.hash;
    if (bytes_remaining >= static_cast<long long>(CHUNK_SIZE))
    {
      meta.size = CHUNK_SIZE;
      bytes_remaining -= CHUNK_SIZE;
    }
    else
    {
      meta.size = static_cast<size_t>(bytes_remaining);
      bytes_remaining = 0;
    }
    ordered_chunks.push_back(meta);
  }

  std::string temp_path = output_path + ".dse_download";
  std::cout << "[DEBUG] Pulling " << ordered_chunks.size() << " chunks from cluster...\n";

  auto transfer_start_time = Clock::now();
  bool download_completed = false;

  try
  {
    download_completed = transfer_manager_->DownloadToFile(ordered_chunks, manifest.plans, temp_path, state);
  }
  catch (const std::exception &e)
  {
    std::cerr << "[ERROR] Download pipeline failure: " << e.what() << "\n";
    return false;
  }
  double transfer_duration_sec = std::chrono::duration<double>(Clock::now() - transfer_start_time).count();

  if (download_completed)
  {
    std::filesystem::rename(temp_path, output_path);
    double total_duration_sec = std::chrono::duration<double>(Clock::now() - total_start_time).count();

    // --- ACCURATE TELEMETRY & BENCHMARK REPORTING ---
    double size_mb = static_cast<double>(manifest.file_size) / (1024.0 * 1024.0);

    std::cout << "\n"
              << "====================================================\n"
              << "           DOWNLOAD PERFORMANCE TELEMETRY           \n"
              << "====================================================\n"
              << std::fixed << std::setprecision(3)
              << "File Name             : " << filename << "\n"
              << "File Size             : " << size_mb << " MB (" << manifest.file_size << " bytes)\n"
              << "Total Chunks Fetched  : " << ordered_chunks.size() << "\n"
              << "----------------------------------------------------\n"
              << "Metadata Retrieval    : " << plan_duration_sec << " s\n";

    if (transfer_duration_sec > 0.000001)
    {
      double data_plane_mb_s = size_mb / transfer_duration_sec;
      double data_plane_mbps = data_plane_mb_s * 8.0;
      std::cout << "Data Plane Transfer   : " << transfer_duration_sec << " s (" << data_plane_mb_s << " MB/s | " << data_plane_mbps << " Mbps)\n";
    }
    else
    {
      std::cout << "Data Plane Transfer   : " << transfer_duration_sec << " s\n";
    }

    std::cout << "----------------------------------------------------\n"
              << "Total Execution Time  : " << total_duration_sec << " s\n"
              << "End-to-End Throughput : " << (size_mb / total_duration_sec) << " MB/s (" << ((size_mb / total_duration_sec) * 8.0) << " Mbps)\n"
              << "====================================================\n\n";

    return true;
  }
  else
  {
    if (state->load() == TransferState::CANCELED)
    {
      std::cout << "[INFO] Reclaiming pre-allocated SSD space...\n";
      std::error_code fs_error;
      std::filesystem::remove(temp_path, fs_error);
    }
    else if (state->load() == TransferState::PAUSED)
    {
      std::cout << "[INFO] Download paused. Run command again to resume.\n";
    }
    else
    {
      std::cerr << "[ERROR] Download failed.\n";
    }
    return false;
  }
}

bool DSEClient::RemoveFile(const std::string &filename)
{
  std::cout << "[DEBUG] Requesting deletion of '" << filename << "' from Master Node...\n";
  try
  {
    if (metadata_client_->RemoveFile(filename))
    {
      std::cout << "[SUCCESS] File deleted successfully.\n";
      return true;
    }
    else
    {
      std::cerr << "[ERROR] Failed to delete file. It may not exist or access was denied.\n";
      return false;
    }
  }
  catch (const std::exception &e)
  {
    std::cerr << "[ERROR] Control Plane Communication Failed: " << e.what() << "\n";
    return false;
  }
}