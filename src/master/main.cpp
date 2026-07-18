#include "MasterServer.hpp"
#include "PostgresRepository.hpp"
#include "../common/httplib.h"
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
#include <exception> // Required for std::current_exception

// Global state for signal handling and background thread synchronization.
// volatile std::sig_atomic_t ensures atomic access when modified inside a signal handler.
volatile std::sig_atomic_t shutdown_requested = 0;

std::atomic<bool> keep_running{true};
std::mutex gc_mutex;
std::condition_variable gc_cv;

// Thread pool to manage concurrent execution of background deletion tasks.
httplib::ThreadPool gc_thread_pool(16);

// Signal handler sets the flag to notify the event loop of a shutdown request.
void SignalHandler(int signal)
{
  if (signal == SIGINT || signal == SIGTERM)
  {
    shutdown_requested = 1;
  }
}

// Deletes replicas of a single chunk sequentially to manage concurrent socket usage.
bool DeletePhysicalChunkFromWorkers(const OrphanedChunk &orphan)
{
  bool all_success = true;

  for (const auto &loc : orphan.locations)
  {
    httplib::Client worker_client(loc.host, loc.port);
    worker_client.set_connection_timeout(1, 0); // 1-second connection timeout limit
    worker_client.set_read_timeout(2, 0);
    worker_client.set_write_timeout(2, 0);

    auto res = worker_client.Delete("/chunk?hash=" + orphan.hash);

    if (!res || res->status != 200)
    {
      std::cerr << "[GC ERROR] Worker " << loc.host << ":" << loc.port << " failed to delete " << orphan.hash << "\n";
      all_success = false;
    }
  }

  return all_success;
}

// Processes deletion of a batch of chunks concurrently by dispatching tasks to the thread pool.
std::unordered_set<std::string> PurgeOrphanBatchParallel(const std::vector<OrphanedChunk> &orphans)
{
  std::vector<std::future<std::pair<std::string, bool>>> futures;

  for (const auto &orphan : orphans)
  {
    // Use a shared promise to retrieve results from tasks queued in the thread pool.
    auto promise = std::make_shared<std::promise<std::pair<std::string, bool>>>();
    futures.push_back(promise->get_future());

    gc_thread_pool.enqueue([promise, orphan]()
                           {
        // Catch any exceptions to prevent blocking on the associated future.
        try {
            bool success = DeletePhysicalChunkFromWorkers(orphan);
            promise->set_value({orphan.hash, success});
        } catch (...) {
            promise->set_exception(std::current_exception());
        } });
  }

  std::unordered_set<std::string> successfully_deleted_hashes;
  for (auto &fut : futures)
  {
    try
    {
      // Blocks until the task completes; retrieves the result or rethrows exceptions.
      auto result = fut.get();
      if (result.second)
      {
        successfully_deleted_hashes.insert(result.first);
      }
    }
    // Isolate exceptions to individual tasks so successful deletions can still be committed.
    catch (const std::exception &e)
    {
      std::cerr << "[GC ERROR] Deletion task threw exception: " << e.what() << "\n";
    }
    catch (...)
    {
      std::cerr << "[GC ERROR] Deletion task threw an unknown exception.\n";
    }
  }

  return successfully_deleted_hashes;
}

// Background thread daemon responsible for periodic cleanup of orphaned chunks.
void GarbageCollectorDaemon(IMetadataRepository *db)
{
  int expiry_hours = 24;
  int batch_size = 100;

  std::cout << "[GC DAEMON] Background Garbage Collector successfully started.\n";

  while (keep_running)
  {
    // Prevent database or network errors from terminating the background thread.
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

        // Delete the current batch of orphaned chunks from storage nodes.
        std::unordered_set<std::string> successful_purges = PurgeOrphanBatchParallel(orphans);
        bool delete_success = true;

        if (!successful_purges.empty())
        {
          // Convert set of successful deletions to a vector for the bulk database update.
          std::vector<std::string> purged_list;
          for (const auto &hash : successful_purges)
          {
            purged_list.push_back(hash);
          }
          // Commit successful deletions in a single database operation.
          delete_success = db->DeleteChunkRecordsBulk(purged_list);

          total_deleted += delete_success ? successful_purges.size() : 0;
        }

        // Revert tombstone states in the database if deletion fails.
        if (!delete_success)
        {
          for (auto &orphan : orphans)
          {
            db->RevertTombstone(orphan.hash);
          }
        }
        else
        {
          // Identify and revert only the chunks that failed deletion.
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

    // Sleep for 24 hours, or wake up early if notified of a shutdown.
    std::unique_lock<std::mutex> lock(gc_mutex);
    std::cout << "[GC DAEMON] Sleeping for 24 hours (Interruptible)...\n";
    gc_cv.wait_for(lock, std::chrono::hours(24), []
                   { return !keep_running; });
  }
  std::cout << "[GC DAEMON] Background thread exiting gracefully.\n";
}

// Program entry point
int main(int argc, char *argv[])
{
  int port = 8081;

  if (argc > 1)
  {
    port = std::stoi(argv[1]);
  }

  // Set up handlers for termination signals.
  std::signal(SIGINT, SignalHandler);  // Catches Ctrl+C
  std::signal(SIGTERM, SignalHandler); // Catches Docker kill commands

  const char *env_db_url = std::getenv("DATABASE_URL");
  std::string conn_str = env_db_url ? env_db_url : "postgresql://postgres:password@127.0.0.1:5432/metadata";

  const char *env_buckets = std::getenv("NUM_BUCKETS");
  const char *env_cluster_secret = std::getenv("CLUSTER_SECRET");
  const char *env_session_secret = std::getenv("SESSION_SECRET");

  std::string cluster_secret = env_cluster_secret ? env_cluster_secret : "my_shared_secret_key";
  std::string session_secret = env_session_secret ? env_session_secret : "my_shared_secret_key";

  int num_buckets = 256;
  if (env_buckets)
  {
    try
    {
      num_buckets = std::stoi(env_buckets);
    }
    catch (...)
    {
      num_buckets = 256;
    }
  }

  PostgresRepository repo(conn_str, num_buckets);

  // Start the background Garbage Collector thread.
  std::thread gc_thread(GarbageCollectorDaemon, &repo);

  // Run the HTTP server in a separate thread to allow the main thread to monitor signals.
  MasterServer server(port, &repo, cluster_secret, session_secret);
  std::thread server_thread([&server, port]()
                            { server.Listen(port); });

  std::cout << "[INIT] Master Node booting up on port " << port << "...\n";
  std::cout << "[INIT] Using PostgreSQL connection string from configuration.\n";

  // Main event loop that periodically checks the shutdown flag.
  while (shutdown_requested == 0)
  {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  // Graceful shutdown sequence.
  std::cout << "\n[SHUTDOWN] Shutdown requested. Stopping HTTP server...\n";
  server.Stop();

  if (server_thread.joinable())
  {
    server_thread.join();
  }

  std::cout << "[SHUTDOWN] Stopping GC background daemon...\n";
  keep_running = false;
  gc_cv.notify_all(); // Wake up the background daemon thread to allow it to exit.

  if (gc_thread.joinable())
  {
    gc_thread.join();
  }

  std::cout << "[SHUTDOWN] Master Node successfully exited.\n";
  return 0;
}