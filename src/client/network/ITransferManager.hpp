#pragma once
#include "../file/IFileSlicer.hpp"
#include "../state/ILocalStateManager.hpp"
#include "../../common/models.hpp"
#include <vector>
#include <string>
#include <atomic>
#include <utility>

// Abstract boundary defining data plane network transmissions.
class ITransferManager
{
public:
  virtual ~ITransferManager() = default;

  // Uploads missing chunks to the physical cluster.
  // Utilizes a flat, pre-correlated array (pending_tasks) containing the exact
  // SSD index mapped to its validated network routing plan.
  virtual bool Upload(IFileSlicer *slicer, ILocalStateManager *state_manager,
                      const std::string &filepath, size_t chunk_size,
                      const std::vector<std::pair<int, ChunkRoutingPlan>> &pending_tasks,
                      std::atomic<TransferState> *state) = 0;

  // Downloads and reassembles chunks from the physical cluster using Direct Memory Access limits.
  virtual bool DownloadToFile(const std::vector<DownloadChunkMeta> &ordered_chunks,
                              const std::vector<ChunkRoutingPlan> &plans,
                              const std::string &output_path,
                              std::atomic<TransferState> *state) = 0;
};