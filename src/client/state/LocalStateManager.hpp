#pragma once
#include "ILocalStateManager.hpp"
#include <filesystem>
#include <string>

// Concrete implementation handling filesystem I/O for client state persistence.
class LocalStateManager : public ILocalStateManager
{
private:
  std::filesystem::path workspace_dir;
  std::filesystem::path transfers_dir;
  std::filesystem::path config_file;

  // Helper utility to safely resolve the user's home directory across POSIX/Windows environments.
  std::filesystem::path GetHomeDirectory();

  // Generates a deterministic, filesystem-safe filename based on the target upload path.
  std::string GetManifestFilename(const std::string &filepath);

public:
  // Constructor automatically provisions the required local directory structure.
  LocalStateManager();

  bool HasActiveUpload(const std::string &filepath) override;
  UploadManifest LoadUploadManifest(const std::string &filepath) override;
  void SaveUploadManifest(const UploadManifest &manifest) override;
  void AppendCompletedIndices(const std::string &filepath, const std::vector<int> &new_indices) override;
  void DeleteUploadManifest(const std::string &filepath) override;

  void SaveAuthToken(const std::string &token) override;
  std::string LoadAuthToken() override;
};