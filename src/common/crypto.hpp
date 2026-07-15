#pragma once
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <openssl/hmac.h>
#include <vector>
#include <string>
#include <stdexcept>
#include <cstring>
#include <sstream>
#include <iomanip>
#include <chrono>

constexpr int PBKDF2_ITERATIONS = 100000;

// ============================================================================
// DATA PLANE CRYPTOGRAPHY (AES-256-CBC)
// ============================================================================

// Encrypts raw binary data utilizing AES-256-CBC and prepends a randomized 16-byte IV.
// key must point to a valid 32-byte AES-256 key.
inline std::vector<char> EncryptBytes(const std::vector<char> &raw_data, const unsigned char *AES_KEY)
{
  unsigned char iv[16];
  if (RAND_bytes(iv, sizeof(iv)) != 1)
  {
    throw std::runtime_error("Failed to generate random IV");
  }

  EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
  if (!ctx)
    throw std::runtime_error("Failed to create EVP context");

  if (EVP_EncryptInit_ex(ctx, EVP_aes_256_cbc(), nullptr, AES_KEY, iv) != 1)
  {
    EVP_CIPHER_CTX_free(ctx);
    throw std::runtime_error("Encryption init failed");
  }

  std::vector<char> final_output(sizeof(iv) + raw_data.size() + EVP_MAX_BLOCK_LENGTH);

  // Prepend the IV to the beginning of the output vector
  std::memcpy(final_output.data(), iv, sizeof(iv));

  int len = 0;
  int ciphertext_len = 0;

  unsigned char *ciphertext_ptr = reinterpret_cast<unsigned char *>(final_output.data() + sizeof(iv));

  if (EVP_EncryptUpdate(ctx, ciphertext_ptr, &len,
                        reinterpret_cast<const unsigned char *>(raw_data.data()), raw_data.size()) != 1)
  {
    EVP_CIPHER_CTX_free(ctx);
    throw std::runtime_error("Encryption failed");
  }
  ciphertext_len = len;

  if (EVP_EncryptFinal_ex(ctx, ciphertext_ptr + len, &len) != 1)
  {
    EVP_CIPHER_CTX_free(ctx);
    throw std::runtime_error("Encryption finalization failed");
  }
  ciphertext_len += len;

  EVP_CIPHER_CTX_free(ctx);
  final_output.resize(sizeof(iv) + ciphertext_len);

  return final_output;
}

// Extracts the prepended 16-byte IV and decrypts the remaining AES-256-CBC payload.
inline std::vector<char> DecryptBytes(const std::vector<char> &file_data, const unsigned char *AES_KEY)
{
  if (file_data.size() < 16)
  {
    throw std::runtime_error("Corrupted payload: Insufficient size for IV");
  }

  unsigned char iv[16];
  std::memcpy(iv, file_data.data(), sizeof(iv));

  EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
  if (!ctx)
    throw std::runtime_error("Failed to create EVP context");

  if (EVP_DecryptInit_ex(ctx, EVP_aes_256_cbc(), nullptr, AES_KEY, iv) != 1)
  {
    EVP_CIPHER_CTX_free(ctx);
    throw std::runtime_error("Decryption init failed");
  }

  const unsigned char *ciphertext_ptr = reinterpret_cast<const unsigned char *>(file_data.data() + sizeof(iv));
  int ciphertext_size = file_data.size() - sizeof(iv);

  std::vector<char> decrypted_data(ciphertext_size);
  int len = 0;
  int plaintext_len = 0;

  if (EVP_DecryptUpdate(ctx, reinterpret_cast<unsigned char *>(decrypted_data.data()), &len,
                        ciphertext_ptr, ciphertext_size) != 1)
  {
    EVP_CIPHER_CTX_free(ctx);
    throw std::runtime_error("Decryption failed");
  }
  plaintext_len = len;

  if (EVP_DecryptFinal_ex(ctx, reinterpret_cast<unsigned char *>(decrypted_data.data()) + len, &len) != 1)
  {
    EVP_CIPHER_CTX_free(ctx);
    throw std::runtime_error("Decryption finalization failed (Padding error or Corrupted Data)");
  }
  plaintext_len += len;

  EVP_CIPHER_CTX_free(ctx);
  decrypted_data.resize(plaintext_len);

  return decrypted_data;
}

// Computes the SHA-256 hash of a string using the forward-compatible OpenSSL EVP interface.
inline std::string sha256(const std::string &str)
{
  EVP_MD_CTX *context = EVP_MD_CTX_new();
  if (!context)
  {
    throw std::runtime_error("Failed to create EVP_MD_CTX");
  }

  if (EVP_DigestInit_ex(context, EVP_sha256(), nullptr) != 1)
  {
    EVP_MD_CTX_free(context);
    throw std::runtime_error("EVP_DigestInit_ex failed");
  }

  if (EVP_DigestUpdate(context, str.c_str(), str.size()) != 1)
  {
    EVP_MD_CTX_free(context);
    throw std::runtime_error("EVP_DigestUpdate failed");
  }

  unsigned char hash[EVP_MAX_MD_SIZE];
  unsigned int length_of_hash = 0;

  if (EVP_DigestFinal_ex(context, hash, &length_of_hash) != 1)
  {
    EVP_MD_CTX_free(context);
    throw std::runtime_error("EVP_DigestFinal_ex failed");
  }

  EVP_MD_CTX_free(context);

  std::stringstream ss;
  for (unsigned int i = 0; i < length_of_hash; ++i)
  {
    ss << std::hex << std::setfill('0') << std::setw(2) << static_cast<int>(hash[i]);
  }
  return ss.str();
}

// Generates an HMAC-SHA256 signature to cryptographically sign session tokens.
inline std::string hmac_sha256(const std::string &key, const std::string &data)
{
  unsigned char hash[EVP_MAX_MD_SIZE];
  unsigned int len = 0;

  HMAC(EVP_sha256(), key.c_str(), key.length(),
       reinterpret_cast<const unsigned char *>(data.c_str()), data.length(),
       hash, &len);

  std::stringstream ss;
  for (unsigned int i = 0; i < len; i++)
  {
    ss << std::hex << std::setfill('0') << std::setw(2) << static_cast<int>(hash[i]);
  }
  return ss.str();
}

// Constructs a stateless session token comprising the User ID,
// a 24-hour expiration timestamp, and an HMAC-SHA256 signature.
inline std::string GenerateSessionToken(int user_id, const std::string &secret_key)
{
  long long expires_at = std::chrono::duration_cast<std::chrono::seconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count() +
                         86400; // 24-hour TTL

  std::string payload = std::to_string(user_id) + "." + std::to_string(expires_at);
  std::string signature = hmac_sha256(secret_key, payload);

  return payload + "." + signature;
}

// Generates a cryptographically secure random salt (returned as a hex string).
inline std::string GenerateSalt(int byte_length = 16)
{
  std::vector<unsigned char> salt(byte_length);
  if (RAND_bytes(salt.data(), byte_length) != 1)
  {
    throw std::runtime_error("Failed to generate random salt");
  }

  std::stringstream ss;
  for (int i = 0; i < byte_length; ++i)
  {
    ss << std::hex << std::setfill('0') << std::setw(2) << static_cast<int>(salt[i]);
  }
  return ss.str();
}

// Computes a computationally expensive PBKDF2 password hash to deter brute-force attacks.
// Utilizes 100,000 iterations of HMAC-SHA256.
inline std::string HashPasswordPBKDF2(const std::string &password, const std::string &salt_hex)
{
  const int iterations = PBKDF2_ITERATIONS;
  const int key_length = 32;
  unsigned char hash[key_length];

  if (PKCS5_PBKDF2_HMAC(password.c_str(), password.size(),
                        reinterpret_cast<const unsigned char *>(salt_hex.c_str()), salt_hex.size(),
                        iterations, EVP_sha256(), key_length, hash) != 1)
  {
    throw std::runtime_error("PBKDF2 hashing failed");
  }

  std::stringstream ss;
  for (int i = 0; i < key_length; ++i)
  {
    ss << std::hex << std::setfill('0') << std::setw(2) << static_cast<int>(hash[i]);
  }
  return ss.str();
}

inline bool ConstantTimeEquals(const std::string &a, const std::string &b)
{
  if (a.size() != b.size())
  {
    return false;
  }
  return CRYPTO_memcmp(a.data(), b.data(), a.size()) == 0;
}