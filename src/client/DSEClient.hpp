#pragma once

#include "crypto/ICryptoEngine.hpp"
#include "file/IFileSlicer.hpp"
#include "network/IMetadataClient.hpp"
#include "network/ITransferManager.hpp"
#include "state/ILocalStateManager.hpp"
#include "../common/models.hpp"

#include <memory>
#include <string>
#include <atomic>

// The primary orchestrator for the Distributed Storage Engine client.
// Implements the Facade design pattern to decouple user interface commands (CLI/GUI)
// from the underlying physical subsystems (I/O, Cryptography, Network, State).
class DSEClient
{
private:
  // Subsystem Dependencies
  std::shared_ptr<ICryptoEngine> crypto_engine_;
  std::unique_ptr<IFileSlicer> slicer_;
  std::unique_ptr<IMetadataClient> metadata_client_;
  std::unique_ptr<ITransferManager> transfer_manager_;
  std::unique_ptr<ILocalStateManager> state_manager_;

public:
  // Constructs the sub-systems and injects the required dependencies.
  // The master_url defines the Control Plane load balancer endpoint.
  explicit DSEClient(const std::string &master_url, std::unique_ptr<ILocalStateManager> state_manager);

  // --- Authentication Operations ---

  // Registers a new user schema on the Control Plane.
  bool Signup(const std::string &email, const std::string &password);

  // Authenticates and stores the resulting JWT/Session token in the metadata client.
  bool Login(const std::string &email, const std::string &password);

  // --- Data Plane Operations ---

  // Executes the two-pass streaming upload pipeline.
  // Supports pause/resume via the injected atomic TransferState flag.
  // Handles local state manifests to guarantee crash-consistent resumes.
  bool UploadFile(const std::string &filepath, std::atomic<TransferState> *state);

  // Executes the zero-copy Direct Memory Access download pipeline.
  // Supports pause/cancel/resume via the injected atomic TransferState flag.
  // Uses the .nexusdownload temporary extension to safeguard incomplete files.
  bool DownloadFile(const std::string &filename, const std::string &output_path, std::atomic<TransferState> *state);

  // Removes a file from the storage system.
  bool RemoveFile(const std::string &filename);
};