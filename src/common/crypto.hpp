#pragma once
#include <openssl/evp.h>
#include <openssl/rand.h> // REQUIRED FOR RAND_bytes
#include <openssl/sha.h>
#include <vector>
#include <stdexcept>
#include <cstring>

// 32-byte Key (In prod, load from ENV)
const unsigned char AES_KEY[32] = {
    'm', 'y', '_', 'u', 'l', 't', 'r', 'a', '_', 's', 'e', 'c', 'u', 'r', 'e', '_',
    '3', '2', '_', 'b', 'y', 't', 'e', '_', 'a', 'e', 's', '_', 'k', 'e', 'y', '!'};

inline std::vector<char> EncryptBytes(const std::vector<char> &raw_data)
{
  // 1. Generate a cryptographically secure random 16-byte IV
  unsigned char iv[16];
  if (RAND_bytes(iv, sizeof(iv)) != 1)
  {
    throw std::runtime_error("Failed to generate random IV");
  }

  EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
  if (!ctx)
    throw std::runtime_error("Failed to create EVP context");

  // Initialize encryption with the RANDOM IV
  if (EVP_EncryptInit_ex(ctx, EVP_aes_256_cbc(), NULL, AES_KEY, iv) != 1)
  {
    EVP_CIPHER_CTX_free(ctx);
    throw std::runtime_error("Encryption init failed");
  }

  // The output buffer needs space for: The IV (16 bytes) + Ciphertext + Padding (up to 16 bytes)
  std::vector<char> final_output(sizeof(iv) + raw_data.size() + EVP_MAX_BLOCK_LENGTH);

  // 2. Prepend the IV to the very beginning of our output vector!
  std::memcpy(final_output.data(), iv, sizeof(iv));

  int len = 0;
  int ciphertext_len = 0;

  // 3. Encrypt the data, writing it into the vector AFTER the 16-byte IV
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

  // Resize to exact size: 16 (IV) + Exact Ciphertext Size
  final_output.resize(sizeof(iv) + ciphertext_len);

  return final_output;
}

inline std::vector<char> DecryptBytes(const std::vector<char> &file_data)
{
  if (file_data.size() < 16)
  {
    throw std::runtime_error("Corrupted file: Too small to contain IV");
  }

  // 1. Extract the 16-byte IV from the beginning of the file
  unsigned char iv[16];
  std::memcpy(iv, file_data.data(), sizeof(iv));

  EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();
  if (!ctx)
    throw std::runtime_error("Failed to create EVP context");

  // Initialize decryption using the extracted IV
  if (EVP_DecryptInit_ex(ctx, EVP_aes_256_cbc(), NULL, AES_KEY, iv) != 1)
  {
    EVP_CIPHER_CTX_free(ctx);
    throw std::runtime_error("Decryption init failed");
  }

  // Calculate ciphertext boundaries (skipping the first 16 bytes)
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