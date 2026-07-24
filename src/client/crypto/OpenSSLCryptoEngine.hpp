#pragma once
#include "ICryptoEngine.hpp"

class OpenSSLCryptoEngine : public ICryptoEngine
{
public:
  std::string ComputeHash(const std::vector<char> &data) override;
};