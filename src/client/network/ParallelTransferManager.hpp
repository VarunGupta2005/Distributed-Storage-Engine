#pragma once
#include "ITransferManager.hpp"
#include "../crypto/ICryptoEngine.hpp"
#include <memory>

class ParallelTransferManager : public ITransferManager
{
private:
  std::shared_ptr<ICryptoEngine> crypto_engine_;

public:
  explicit ParallelTransferManager(std::shared_ptr<ICryptoEngine> crypto_engine);

  bool Upload(IFileSlicer *slicer, ILocalStateManager *state_manager,
              const std::string &filepath, size_t chunk_size,
              const std::vector<std::pair<int, ChunkRoutingPlan>> &pending_tasks,
              std::atomic<TransferState> *state) override;

  bool DownloadToFile(const std::vector<DownloadChunkMeta> &ordered_chunks,
                      const std::vector<ChunkRoutingPlan> &plans,
                      const std::string &output_path,
                      std::atomic<TransferState> *state) override;
};