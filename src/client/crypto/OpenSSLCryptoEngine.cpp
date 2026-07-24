#include "OpenSSLCryptoEngine.hpp"
#include "../../common/crypto.hpp"

std::string OpenSSLCryptoEngine::ComputeHash(const std::vector<char> &data)
{
  std::string binary_str(data.begin(), data.end());
  return sha256(binary_str); // Delegated to existing backend crypto utility
}