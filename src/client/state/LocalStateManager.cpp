#include "LocalStateManager.hpp"
#include "../../common/json.hpp"
#include <fstream>
#include <iostream>
#include <cstdlib>
#include <system_error>
#include <filesystem>

using json = nlohmann::json;

LocalStateManager::LocalStateManager()
{
  // Sets the local workspace directory structure using .dse_client
  workspace_dir = GetHomeDirectory() / ".dse_client";
  transfers_dir = workspace_dir / "transfers";
  config_file = workspace_dir / "config.json";

  std::error_code fs_error;
  std::filesystem::create_directories(transfers_dir, fs_error);
  if (fs_error)
  {
    std::cerr << "[WARNING] Failed to initialize local workspace directories: " << fs_error.message() << "\n";
  }
}

std::filesystem::path LocalStateManager::GetHomeDirectory()
{
  const char *home_path = std::getenv("HOME");
  if (!home_path)
  {
    home_path = std::getenv("USERPROFILE"); // Fallback for Windows environments
  }
  return home_path ? std::filesystem::path(home_path) : std::filesystem::current_path();
}

std::string LocalStateManager::GetManifestFilename(const std::string &filepath)
{
  std::string file_basename = std::filesystem::path(filepath).filename().string();
  return file_basename + ".manifest.json";
}

bool LocalStateManager::HasActiveUpload(const std::string &filepath)
{
  std::filesystem::path manifest_path = transfers_dir / GetManifestFilename(filepath);
  std::error_code fs_error;

  return std::filesystem::exists(manifest_path, fs_error) && std::filesystem::is_regular_file(manifest_path, fs_error);
}

void LocalStateManager::AppendCompletedIndices(const std::string &filepath, const std::vector<int> &new_indices)
{
  std::string log_filename = std::filesystem::path(filepath).filename().string() + ".completed.log";
  std::filesystem::path full_log_path = transfers_dir / log_filename;

  // Opens stream in append mode to append integer indices without altering existing records
  std::ofstream log_stream(full_log_path, std::ios::app);
  if (!log_stream.is_open())
  {
    return;
  }

  for (int index : new_indices)
  {
    log_stream << index << "\n";
  }
}

UploadManifest LocalStateManager::LoadUploadManifest(const std::string &filepath)
{
  std::string file_basename = std::filesystem::path(filepath).filename().string();
  std::ifstream json_stream(transfers_dir / (file_basename + ".manifest.json"));

  if (!json_stream.is_open())
  {
    throw std::runtime_error("Manifest file is inaccessible or missing.");
  }

  json manifest_json;
  json_stream >> manifest_json;

  UploadManifest manifest;
  manifest.filepath = manifest_json.at("filepath").get<std::string>();
  manifest.total_size = manifest_json.at("total_size").get<long long>();
  manifest.temp_modified_timestamp = manifest_json.at("temp_modified_timestamp").get<long long>();

  // Deserializes session_timestamp to enforce the 24-hour server Garbage Collection TTL check
  manifest.session_timestamp = manifest_json.value("session_timestamp", 0LL);

  manifest.chunk_hashes = manifest_json.at("chunk_hashes").get<std::vector<std::string>>();

  // Allocates memory for the bitset matching the known chunk sequence length
  manifest.completed_chunks.assign(manifest.chunk_hashes.size(), false);

  std::ifstream log_stream(transfers_dir / (file_basename + ".completed.log"));
  int completed_index;

  // Reads completed chunk indices line-by-line to populate the RAM bitset
  while (log_stream >> completed_index)
  {
    if (completed_index >= 0 && static_cast<size_t>(completed_index) < manifest.completed_chunks.size())
    {
      manifest.completed_chunks[completed_index] = true;
    }
  }

  return manifest;
}

void LocalStateManager::SaveUploadManifest(const UploadManifest &manifest)
{
  std::string file_basename = std::filesystem::path(manifest.filepath).filename().string();
  std::ofstream json_stream(transfers_dir / (file_basename + ".manifest.json"));

  if (!json_stream.is_open())
  {
    return;
  }

  // Serializes static metadata. Dynamic upload progress is tracked via AppendCompletedIndices.
  json manifest_json = {
      {"filepath", manifest.filepath},
      {"total_size", manifest.total_size},
      {"temp_modified_timestamp", manifest.temp_modified_timestamp},
      {"session_timestamp", manifest.session_timestamp},
      {"chunk_hashes", manifest.chunk_hashes}};

  json_stream << manifest_json.dump(4);
}

void LocalStateManager::DeleteUploadManifest(const std::string &filepath)
{
  std::string file_basename = std::filesystem::path(filepath).filename().string();
  std::error_code fs_error;

  // Deletes both the static JSON blueprint and the dynamic progress log upon commit completion
  std::filesystem::path json_path = transfers_dir / (file_basename + ".manifest.json");
  std::filesystem::remove(json_path, fs_error);

  std::filesystem::path log_path = transfers_dir / (file_basename + ".completed.log");
  std::filesystem::remove(log_path, fs_error);
}

// --- AUTH TOKEN MANAGEMENT ---

void LocalStateManager::SaveAuthToken(const std::string &token)
{
  json config_json;

  std::ifstream config_in_stream(config_file);
  if (config_in_stream.is_open())
  {
    try
    {
      config_in_stream >> config_json;
    }
    catch (...)
    {
      // Ignores parse errors to permit file re-initialization
    }
    config_in_stream.close();
  }

  config_json["auth_token"] = token;

  std::ofstream config_out_stream(config_file);
  if (config_out_stream.is_open())
  {
    config_out_stream << config_json.dump(4);
  }
}

std::string LocalStateManager::LoadAuthToken()
{
  std::ifstream config_in_stream(config_file);
  if (!config_in_stream.is_open())
  {
    return "";
  }

  try
  {
    json config_json;
    config_in_stream >> config_json;
    if (config_json.contains("auth_token"))
    {
      return config_json["auth_token"].get<std::string>();
    }
  }
  catch (...)
  {
    // Returns empty string if the configuration file is corrupt or unreadable
  }

  return "";
}