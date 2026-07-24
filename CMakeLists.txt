cmake_minimum_required(VERSION 3.10)
project(DistributedStorageEngine VERSION 1.0 LANGUAGES CXX)

# Enforce C++17 standard (Required for std::filesystem and structured bindings)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_CXX_EXTENSIONS OFF)

# Ensure pthread is linked for multithreading (cpp-httplib and std::async)
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

# Locate OpenSSL (Required for AES-256-CBC, PBKDF2, and SHA-256 in crypto.hpp)
find_package(OpenSSL REQUIRED)

# ---------------------------------------------------------
# EXECUTABLE 1: Master Node (Control Plane)
# ---------------------------------------------------------
add_executable(master_server
    src/master/main.cpp
)

target_include_directories(master_server PRIVATE
    ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(master_server
    PRIVATE
    Threads::Threads
    OpenSSL::Crypto
    pqxx
    pq
)

# ---------------------------------------------------------
# EXECUTABLE 2: Worker Node (Data Plane)
# ---------------------------------------------------------
add_executable(worker_server
    src/worker/main.cpp
)

target_include_directories(worker_server PRIVATE
    ${CMAKE_SOURCE_DIR}/src
)

target_link_libraries(worker_server
    PRIVATE
    Threads::Threads
    OpenSSL::Crypto
)

# ---------------------------------------------------------
# EXECUTABLE 3: Native CLI Client
# ---------------------------------------------------------
add_executable(client_cli
    src/client/main.cpp
    src/client/cli/CLI.cpp
    src/client/DSEClient.cpp
    src/client/crypto/OpenSSLCryptoEngine.cpp
    src/client/file/LocalFileSlicer.cpp
    src/client/state/LocalStateManager.cpp
    src/client/network/MasterHttpClient.cpp
    src/client/network/ParallelTransferManager.cpp
)

# Enforce target-specific include directories
target_include_directories(client_cli PRIVATE
    ${CMAKE_SOURCE_DIR}/src
)

# Link the client to multithreading and OpenSSL
target_link_libraries(client_cli
    PRIVATE
    Threads::Threads
    OpenSSL::Crypto
)