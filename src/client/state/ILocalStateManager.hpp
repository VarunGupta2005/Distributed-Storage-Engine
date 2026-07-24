#pragma once
#include "../../common/models.hpp"
#include <string>

// Abstract boundary for local filesystem state serialization.
class ILocalStateManager
{
public:
  virtual ~ILocalStateManager() = default;

  virtual bool HasActiveUpload(const std::string &filepath) = 0;
  virtual UploadManifest LoadUploadManifest(const std::string &filepath) = 0;
  virtual void SaveUploadManifest(const UploadManifest &manifest) = 0;

  // NEW: Appends chunk indices to the end of the log file without touching historical data
  virtual void AppendCompletedIndices(const std::string &filepath, const std::vector<int> &new_indices) = 0;
  virtual void DeleteUploadManifest(const std::string &filepath) = 0;

  virtual void SaveAuthToken(const std::string &token) = 0;
  virtual std::string LoadAuthToken() = 0;
};