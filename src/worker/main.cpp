#include "WorkerServer.hpp"
#include "LocalDiskStorage.hpp"
#include "../common/httplib.h"
#include "../common/json.hpp" // nlohmann/json

#include <iostream>
#include <string>
#include <cstdlib>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <chrono>
#include <csignal>
#include <filesystem>

// Global state for signal handling and background thread synchronization.
// volatile std::sig_atomic_t ensures atomic access when modified inside a signal handler.
volatile std::sig_atomic_t shutdown_requested = 0;

std::atomic<bool> keep_running{true};
std::mutex hb_mutex;
std::condition_variable hb_cv;

// Signal handler sets the flag to notify the event loop of a shutdown request.
void SignalHandler(int signal)
{
  if (signal == SIGINT || signal == SIGTERM)
  {
    shutdown_requested = 1;
  }
}

// Background daemon thread for telemetry.
// Periodically transmits the worker's network identities and physical storage capacity to the Control Plane.
void HeartbeatDaemon(const std::string &master_lb, const std::string &internal_host,
                     const std::string &advertised_host, int port, const std::string &storage_dir)
{
  std::cout << "[HEARTBEAT] Daemon started. Targeting Control Plane at " << master_lb << ":80\n";

  while (keep_running)
  {
    try
    {
      // Query the filesystem for available disk space.
      std::error_code ec;
      auto space_info = std::filesystem::space(storage_dir, ec);
      long long free_space = ec ? 0 : static_cast<long long>(space_info.available);

      // Construct the JSON payload containing internal and advertised network coordinates.
      nlohmann::json payload = {
          {"internal_host", internal_host},
          {"internal_port", port},
          {"advertised_host", advertised_host},
          {"advertised_port", port},
          {"free_space", free_space}};

      // Transmit telemetry to the Master Node / Load Balancer.
      httplib::Client cli(master_lb, 80);

      // Set a short connection timeout to prevent blocking if the target is unreachable.
      cli.set_connection_timeout(2, 0);

      // Include the cluster secret for mutual authentication.
      httplib::Headers headers = {
          {"X-Cluster-Secret", "my_shared_secret_key"}};

      auto res = cli.Post("/heartbeat", headers, payload.dump(), "application/json");

      if (!res || res->status != 200)
      {
        std::cerr << "[HEARTBEAT WARNING] Failed to establish telemetry link with Control Plane.\n";
      }
    }
    catch (const std::exception &e)
    {
      std::cerr << "[HEARTBEAT ERROR] Exception during telemetry cycle: " << e.what() << "\n";
    }

    // Sleep for 5 seconds, or wake up early if notified of a shutdown.
    std::unique_lock<std::mutex> lock(hb_mutex);
    hb_cv.wait_for(lock, std::chrono::seconds(5), []
                   { return !keep_running; });
  }
  std::cout << "[HEARTBEAT] Daemon exiting gracefully.\n";
}

// Program entry point
int main(int argc, char *argv[])
{
  int port = 9001;

  if (argc > 1)
  {
    port = std::stoi(argv[1]);
  }

  // Set up handlers for termination signals.
  std::signal(SIGINT, SignalHandler);
  std::signal(SIGTERM, SignalHandler);

  // Read infrastructure configuration from environment variables.
  const char *env_master = std::getenv("MASTER_LB_HOST");
  std::string master_lb = env_master ? env_master : "localhost";

  const char *env_internal = std::getenv("INTERNAL_HOST");
  std::string internal_host = env_internal ? env_internal : "127.0.0.1";

  const char *env_advertised = std::getenv("ADVERTISED_HOST");
  std::string advertised_host = env_advertised ? env_advertised : "127.0.0.1";

  std::string storage_dir = "./data/node_" + std::to_string(port);

  const char *key = std::getenv("AES_KEY");
  if (!key)
  {
    std::cerr << "[INIT ERROR] AES_KEY environment variable not set.\n";
    return EXIT_FAILURE;
  }
  if (std::strlen(key) != 32)
  {
    std::cerr << "[INIT ERROR] AES_KEY must be exactly 32 bytes.\n";
    return EXIT_FAILURE;
  }
  const unsigned char *node_key = reinterpret_cast<const unsigned char *>(key);

  // Initialize the physical storage backend.
  LocalDiskStorage storage(storage_dir, node_key);

  // Start the background heartbeat telemetry thread.
  std::thread hb_thread(HeartbeatDaemon, master_lb, internal_host, advertised_host, port, storage_dir);

  // Run the HTTP server in a separate thread to allow the main thread to monitor signals.
  WorkerServer server(&storage);
  std::thread server_thread([&server, port]()
                            { server.Listen(port); });

  std::cout << "[INIT] Worker Node booting up on port " << port << "...\n";
  std::cout << "[INIT] Storage Directory: " << storage_dir << "\n";
  std::cout << "[INIT] Advertised Endpoint: " << advertised_host << ":" << port << "\n";

  // Main event loop that periodically checks the shutdown flag.
  while (shutdown_requested == 0)
  {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  // Graceful shutdown sequence.
  std::cout << "\n[SHUTDOWN] Shutdown requested. Stopping Data Plane server...\n";
  server.Stop();

  if (server_thread.joinable())
  {
    server_thread.join();
  }

  std::cout << "[SHUTDOWN] Stopping Heartbeat telemetry daemon...\n";
  keep_running = false;
  hb_cv.notify_all(); // Wake up the background daemon thread to allow it to exit.

  if (hb_thread.joinable())
  {
    hb_thread.join();
  }

  std::cout << "[SHUTDOWN] Worker Node successfully exited.\n";
  return 0;
}