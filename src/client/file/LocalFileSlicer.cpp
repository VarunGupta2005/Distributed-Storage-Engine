#include "LocalFileSlicer.hpp"
#include "../../common/httplib.h"
#include "../../common/Semaphore.hpp"
#include <stdexcept>
#include <future>
#include <algorithm>

LocalFileSlicer::LocalFileSlicer(std::shared_ptr<ICryptoEngine> crypto_engine)
    : crypto_engine_(std::move(crypto_engine)), current_chunk_size(0) {}

LocalFileSlicer::~LocalFileSlicer()
{
  CloseStream();
}

std::vector<std::string> LocalFileSlicer::ComputeHashes(const std::string &filepath, size_t chunk_size)
{
  std::ifstream file(filepath, std::ios::binary);
  if (!file.is_open())
    throw std::runtime_error("Failed to open file for hashing.");

  // Bounding hardware_concurrency to prevent returning 0, and capping at 8 threads
  // to prevent CPU starvation on high-core-count machines during I/O ops.
  unsigned int thread_count = std::max(1u, std::min(8u, std::thread::hardware_concurrency()));
  httplib::ThreadPool pool(thread_count);

  std::vector<std::future<std::string>> futures;
  Semaphore in_flight_limiter(32);

  std::vector<char> buffer(chunk_size);
  ICryptoEngine *crypto_engine = crypto_engine_.get();

  // The Producer Thread: Streams data from the SSD as fast as the memory limiter allows.
  while (file.read(buffer.data(), buffer.size()) || file.gcount() > 0)
  {
    std::streamsize bytes_read = file.gcount();
    // std::cout << "[DEBUG] Read chunk: " << bytes_read << " bytes\n";
    in_flight_limiter.acquire();
    // std::cout << "[DEBUG] Acquired semaphore\n";

    auto permit = std::make_shared<InFlightPermit>(in_flight_limiter);
    auto chunk_data = std::make_shared<std::vector<char>>(buffer.begin(), buffer.begin() + bytes_read);

    auto promise = std::make_shared<std::promise<std::string>>();
    futures.push_back(promise->get_future());

    // The Consumer Thread: Computes the SHA-256 hash.
    pool.enqueue([promise, chunk_data, permit, crypto_engine]() mutable
                 {
            try {
              // std::cout << "[DEBUG] Hash worker started\n";
                std::string hash = crypto_engine->ComputeHash(*chunk_data);
                // std::cout << "[DEBUG] Hash computed: "
                          // << hash << "\n";
                chunk_data.reset();
                // std::cout << "[DEBUG] chunk_data reset\n"; 
                permit.reset(); 
                // std::cout << "[DEBUG] permit reset\n";
                promise->set_value(hash);
                // std::cout << "[DEBUG] promise set\n";

            } catch (...) {
                chunk_data.reset();
                permit.reset();
                promise->set_exception(std::current_exception());
            } });
  }

  std::vector<std::string> chunk_hashes;
  // std::cout << "[DEBUG] Starting future collection\n";
  for (auto &fut : futures)
  {
    // std::cout << "[DEBUG] Calling fut.get()\n";
    chunk_hashes.push_back(fut.get());
    // std::cout << "[DEBUG] fut.get() returned\n";
  }

  pool.shutdown();
  // std::cout << "[DEBUG] Pool shutdown completed\n";
  // std::cout << "[DEBUG] Returning from ComputeHashes\n";
  return chunk_hashes;
}

void LocalFileSlicer::OpenStream(const std::string &filepath, size_t chunk_size)
{
  CloseStream();
  file_stream.open(filepath, std::ios::binary);
  if (!file_stream.is_open())
    throw std::runtime_error("Failed to open file stream.");

  current_chunk_size = chunk_size;
}

std::vector<char> LocalFileSlicer::ReadChunk(int index, size_t chunk_size)
{
  std::streampos offset = static_cast<std::streampos>(index) * chunk_size;
  file_stream.seekg(offset, std::ios::beg);

  std::vector<char> buffer(chunk_size);
  file_stream.read(buffer.data(), buffer.size());
  std::streamsize bytes_read = file_stream.gcount();

  // Trims the buffer to the exact bytes read (critical for the final tail chunk)
  buffer.resize(bytes_read);
  return buffer;
}

void LocalFileSlicer::CloseStream()
{
  if (file_stream.is_open())
  {
    file_stream.close();
  }
}