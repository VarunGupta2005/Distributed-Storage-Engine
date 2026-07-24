#pragma once
#include "IMetadataClient.hpp"
#include <string>

// Concrete implementation of the Control Plane metadata client.
// Manages stateless HTTP REST communication with the Master Node Load Balancer.
class MasterHttpClient : public IMetadataClient
{
private:
  std::string master_url_;
  std::string auth_token_; // Preserves the JWT/Session token for injection into Authorization headers

public:
  explicit MasterHttpClient(const std::string &master_url);

  void SetAuthToken(const std::string &token) override;

  bool CreateUser(const std::string &email, const std::string &password) override;
  std::string Login(const std::string &email, const std::string &password) override;

  std::vector<ChunkRoutingPlan> InitializeUpload(const std::vector<std::string> &chunk_hashes) override;
  bool CommitFile(const std::string &filename, long long file_size, const std::vector<std::string> &chunk_hashes) override;

  FileDownloadManifest GetDownloadPlan(const std::string &filename) override;

  bool FileExists(const std::string &filename) override;
  bool RemoveFile(const std::string &filename) override;
};