#pragma once
#include <vector>
#include <string>

// Abstract boundary for CPU-bound hashing operations.
class ICryptoEngine
{
public:
  virtual ~ICryptoEngine() = default;
  virtual std::string ComputeHash(const std::vector<char> &data) = 0;
};