#include "MasterServer.hpp"
#include "PostgresRepository.hpp"
#include <iostream>
#include <string>
#include <cstdlib>
#include <thread>
#include <future>
#include <vector>
#include <unordered_set>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <chrono>
#include <csignal>
#include <algorithm>

// ASYNC-SIGNAL-SAFE GLOBAL STATE (Strict POSIX Compliance)
// volatile std::sig_atomic_t is safe to write inside signal handlers
volatile std::sig_atomic_t shutdown_requested = 0;

std::atomic<bool> keep_running{true};
std::mutex gc_mutex;
std::condition_variable gc_cv;

// Signal handler only sets a primitive flag and returns immediately (No UB)
void SignalHandler(int signal)
{
  if (signal == SIGINT || signal == SIGTERM)
  {
    shutdown_requested = 1;
  }
}

// PARALLEL PHYSICAL DELETION PIPELINES (THE DATA PLANE)
// LEVEL 1: Delete all physical replicas of ONE chunk from its Workers in parallel
bool DeletePhysicalChunkFromWorkers(const OrphanedChunk &orphan)
{
  std::vector<std::future<bool>> futures;

  for (const auto &loc : orphan.locations)
  {
    futures.push_back(std::async(std::launch::async, [loc, orphan]()
                                 {
            httplib::Client worker_client(loc.host, loc.port);
            worker_client.set_connection_timeout(1, 0); // 1-second timeout guard
            
            auto res = worker_client.Delete("/chunk?hash=" + orphan.hash);
            return (res && res->status == 200); }));
  }

  bool all_success = true;
  for (auto &fut : futures)
  {
    if (!fut.get())
    {
      all_success = false;
    }
  }
  return all_success;
}

// LEVEL 2: Delete ALL chunks in the batch concurrently over the network
std::unordered_set<std::string> PurgeOrphanBatchParallel(const std::vector<OrphanedChunk> &orphans)
{
  std::vector<std::future<std::pair<std::string, bool>>> futures;

  for (const auto &orphan : orphans)
  {
    futures.push_back(std::async(std::launch::async, [orphan]() -> std::pair<std::string, bool>
                                 {
            bool success = DeletePhysicalChunkFromWorkers(orphan);
            return {orphan.hash, success}; }));
  }

  std::unordered_set<std::string> successfully_deleted_hashes;
  for (auto &fut : futures)
  {
    auto result = fut.get();
    if (result.second)
    {
      successfully_deleted_hashes.insert(result.first); // O(1) hash insertion
    }
  }

  return successfully_deleted_hashes;
}

// THE BACKGROUND DAEMON THREAD (Fault-Tolerant)
void GarbageCollectorDaemon(IMetadataRepository *db)
{
  int expiry_hours = 24;
  int batch_size = 100;

  std::cout << "[GC DAEMON] Background Garbage Collector successfully started.\n";

  while (keep_running)
  {
    // Exception safety wrapper: Prevents a temporary database or network
    // disconnect from permanently killing the background thread
    try
    {
      std::cout << "[GC DAEMON] Initiating scheduled database sweep...\n";
      long long total_deleted = 0;

      while (keep_running)
      {
        std::vector<OrphanedChunk> orphans = db->GetOrphanedChunks(expiry_hours, batch_size);
        if (orphans.empty())
        {
          break;
        }

        // Parallel physical network purge
        std::unordered_set<std::string> successful_purges = PurgeOrphanBatchParallel(orphans);
        bool delete_success = true;
        if (!successful_purges.empty())
        {
          // Bulk database deletion in a single SQL transaction
          std::vector<std::string> purged_list;
          for (const auto &hash : successful_purges)
          {
            purged_list.push_back(hash);
          }
          delete_success = db->DeleteChunkRecordsBulk(purged_list);

          total_deleted += delete_success ? successful_purges.size() : 0;
        }

        // RECOVERY: Revert failed tombstones.
        // count() on std::unordered_set is an O(1) lookup, reducing loop complexity to O(N)!
        if (!delete_success)
        {
          for (auto &orphan : orphans)
          {
            db->RevertTombstone(orphan.hash);
          }
        }
        else
        {
          for (const auto &orphan : orphans)
          {
            if (successful_purges.count(orphan.hash) == 0)
            {
              db->RevertTombstone(orphan.hash);
            }
          }
        }
      }
      if (total_deleted > 0)
      {
        std::cout << "[GC DAEMON] Sweep finished. Purged " << total_deleted << " physical chunks.\n";
      }
    }
    catch (const std::exception &e)
    {
      std::cerr << "[GC DAEMON ERROR] Exception caught in background loop: " << e.what() << "\n";
      std::cerr << "[GC DAEMON ERROR] Retrying on next scheduled sweep...\n";
    }

    // Interruptible sleep using Condition Variables
    std::unique_lock<std::mutex> lock(gc_mutex);
    std::cout << "[GC DAEMON] Sleeping for 24 hours (Interruptible)...\n";
    gc_cv.wait_for(lock, std::chrono::hours(24), []
                   { return !keep_running; });
  }
  std::cout << "[GC DAEMON] Background thread exiting gracefully.\n";
}

// ENTRY POINT
int main(int argc, char *argv[])
{
  int port = 8081;

  if (argc > 1)
  {
    port = std::stoi(argv[1]);
  }

  // 1. Set up signal handling first thing (POSIX Standard)
  std::signal(SIGINT, SignalHandler);  // Catches Ctrl+C
  std::signal(SIGTERM, SignalHandler); // Catches Docker kill commands

  const char *db_host = std::getenv("DB_HOST") ? std::getenv("DB_HOST") : "127.0.0.1";
  std::string conn_str = "user=postgres password=password host=" + std::string(db_host) + " port=5432 dbname=metadata";

  PostgresRepository repo(conn_str);

  // 2. Start the Background GC Thread
  std::thread gc_thread(GarbageCollectorDaemon, &repo);

  // 3. Start the HTTP Server in a separate thread so main() remains free to monitor signals
  MasterServer server(port, &repo);
  std::thread server_thread([&server, port]()
                            { server.Listen(port); });

  std::cout << "[INIT] Master Node booting up on port " << port << "...\n";
  std::cout << "[INIT] Connecting to PostgreSQL on " << db_host << ":5432...\n";

  // 4. MAIN THREAD EVENT LOOP (100% Async-Signal-Safe)
  // The main thread simply sleeps in 100ms intervals. If a signal arrives,
  // the signal handler writes to 'shutdown_requested' natively.
  while (shutdown_requested == 0)
  {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  // GRACEFUL SHUTDOWN (Executed safely outside of the signal handler context)
  std::cout << "\n[SHUTDOWN] Shutdown requested. Stopping HTTP server...\n";
  server.Stop();

  if (server_thread.joinable())
  {
    server_thread.join();
  }

  std::cout << "[SHUTDOWN] Stopping GC background daemon...\n";
  keep_running = false;
  gc_cv.notify_all(); // Safe to notify now

  if (gc_thread.joinable())
  {
    gc_thread.join();
  }

  std::cout << "[SHUTDOWN] Master Node successfully exited.\n";
  return 0;
}