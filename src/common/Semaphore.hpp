#pragma once
#include <mutex>
#include <condition_variable>

// Standard concurrency primitive enforcing backpressure across asynchronous pipelines.
struct Semaphore
{
  std::mutex mtx;
  std::condition_variable cv;
  int count;

  explicit Semaphore(int c) : count(c) {}

  void acquire()
  {
    std::unique_lock<std::mutex> lock(mtx);
    cv.wait(lock, [this]
            { return count > 0; });
    count--;
  }

  void release()
  {
    std::unique_lock<std::mutex> lock(mtx);
    count++;
    cv.notify_one();
  }
};

// RAII wrapper managing the lifecycle of an in-flight chunk permit.
// Guarantees release upon destruction, particularly during stack unwinding.
struct InFlightPermit
{
  Semaphore &sem;
  explicit InFlightPermit(Semaphore &s) : sem(s) {}
  ~InFlightPermit() { sem.release(); }
};