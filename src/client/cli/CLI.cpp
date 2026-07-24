#include "CLI.hpp"
#include "../DSEClient.hpp"
#include "../state/LocalStateManager.hpp"
#include "../../common/models.hpp"

#include <iostream>
#include <string>
#include <atomic>
#include <csignal>
#include <cstdlib>

// Global pointer required to expose the active transfer state to the OS signal handler.
static std::atomic<TransferState> *global_transfer_state = nullptr;

// Intercepts termination signals to execute graceful data-plane teardown.
void SignalHandler(int signal)
{
  if (signal == SIGINT || signal == SIGTERM)
  {
    if (global_transfer_state)
    {
      // Ctrl+C: Initiate graceful pause (save progress)
      if (global_transfer_state->load() == TransferState::RUNNING)
      {
        std::cout << "\n[SIGNAL] Interrupt detected. Gracefully pausing active transfer and exiting...\n";
        global_transfer_state->store(TransferState::PAUSED);
      }
    }
    else
    {
      std::exit(1);
    }
  }
}

int CLI::Run(int argc, char *argv[])
{
  // 1. Validate basic argument footprint
  if (argc < 2)
  {
    std::cerr << "Usage: client_cli <command> [arguments...]\n"
              << "Commands:\n"
              << "  signup <email> <password>\n"
              << "  login <email> <password>\n"
              << "  upload <local_filepath>\n"
              << "  download <remote_filename> <local_output_path>\n"
              << "  delete <remote_filename>\n";

    return 1;
  }

  std::string command = argv[1];

  // 2. Resolve Master Load Balancer URL from environment variables
  const char *env_master_url = std::getenv("MASTER_LB_HOST");
  std::string master_url = env_master_url ? std::string("http://") + env_master_url : "http://localhost:80";

  // 3. Initialize dependency-injected systems
  auto state_manager = std::make_unique<LocalStateManager>();
  DSEClient client(master_url, std::move(state_manager));

  // 4. Set up OS Signal Handling for data transfer states
  std::atomic<TransferState> transfer_state{TransferState::RUNNING};
  global_transfer_state = &transfer_state;
  std::signal(SIGINT, SignalHandler);
  std::signal(SIGTERM, SignalHandler);

  // 5. Command Routing
  try
  {
    if (command == "signup")
    {
      if (argc < 4)
      {
        std::cerr << "Usage: client_cli signup <email> <password>\n";
        return 1;
      }
      return client.Signup(argv[2], argv[3]) ? 0 : 1;
    }
    else if (command == "login")
    {
      if (argc < 4)
      {
        std::cerr << "Usage: client_cli login <email> <password>\n";
        return 1;
      }
      return client.Login(argv[2], argv[3]) ? 0 : 1;
    }
    else if (command == "upload")
    {
      if (argc < 3)
      {
        std::cerr << "Usage: client_cli upload <local_filepath>\n";
        return 1;
      }
      std::cout << "[INFO] Initiating upload. Press Ctrl+C once to Pause.\n";
      return client.UploadFile(argv[2], &transfer_state) ? 0 : 1;
    }
    else if (command == "download")
    {
      if (argc < 4)
      {
        std::cerr << "Usage: client_cli download <remote_filename> <local_output_path>\n";
        return 1;
      }
      std::cout << "[INFO] Initiating download. Press Ctrl+C once to Pause, twice to Cancel.\n";
      return client.DownloadFile(argv[2], argv[3], &transfer_state) ? 0 : 1;
    }
    else if (command == "delete")
    {
      if (argc < 3)
      {
        std::cerr << "Usage: client_cli delete <remote_filename>\n";
        return 1;
      }
      // Requires authentication just like upload/download
      return client.RemoveFile(argv[2]) ? 0 : 1;
    }
    else
    {
      std::cerr << "[ERROR] Unknown command: " << command << "\n";
      return 1;
    }
  }
  catch (const std::exception &e)
  {
    // THIS WILL CATCH AND PRINT THE SILENT CRASH
    std::cerr << "\n[FATAL EXCEPTION] " << e.what() << "\n";
    return 1;
  }
}