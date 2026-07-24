#pragma once
#include "../common/httplib.h"
#include "../common/crypto.hpp" // Required for sha256()
#include "IStorageBackend.hpp"
#include <string>

class WorkerServer
{
private:
    httplib::Server svr;
    IStorageBackend *storage;

public:
    WorkerServer(IStorageBackend *database) : storage(database)
    {
        // VULNERABILITY FIX 1: Prevent OOM crashes.
        // Drops any incoming connection attempting to send a payload larger than 1MB.
        // 1048576 bytes (1MB) + 1024 bytes (Safety margin for headers/boundaries)
        svr.set_payload_max_length(1048576 + 1024);

        // 1. POST /chunk?hash=abc (Uploads and saves a chunk)
        svr.Post("/chunk", [this](const httplib::Request &req, httplib::Response &res)
                 {
            std::string expected_hash = req.get_param_value("hash");
            if (expected_hash.empty()) {
                res.status = 400;
                return;
            }

            // VULNERABILITY FIX 2: Byzantine Fault Mitigation (Zero-Trust)
            // Cryptographically verify the raw payload matches the declared URI hash.
            std::string actual_hash = sha256(req.body);
            if (actual_hash != expected_hash) {
                std::cerr << "[SECURITY WARNING] Rejected corrupted or forged payload for hash: " << expected_hash << "\n";
                res.status = 400; 
                res.set_content("Hash mismatch. Payload corrupted.", "text/plain");
                return;
            }

            // Convert validated request body bytes to vector for disk I/O
            std::vector<char> data(req.body.begin(), req.body.end());

            if (this->storage->SaveChunk(expected_hash, data)) {
                res.status = 200;
                res.set_content("OK", "text/plain");
            } else {
                res.status = 500;
            } });

        // 2. GET /chunk?hash=abc (Reads and streams a chunk)
        svr.Get("/chunk", [this](const httplib::Request &req, httplib::Response &res)
                {
            std::string hash = req.get_param_value("hash");
            std::vector<char> data = this->storage->LoadChunk(hash);

            if (!data.empty()) {
                res.status = 200;
                res.set_content(std::string(data.begin(), data.end()), "application/octet-stream");
            } else {
                res.status = 404; 
            } });

        // 3. DELETE /chunk?hash=abc (Purges a physical chunk - Called by Master GC)
        svr.Delete("/chunk", [this](const httplib::Request &req, httplib::Response &res)
                   {
            std::string hash = req.get_param_value("hash");
            if (this->storage->DeleteChunk(hash)) {
                res.status = 200;
                res.set_content("DELETED", "text/plain");
            } else {
                res.status = 404;
            } });
    }

    void Listen(int port)
    {
        svr.listen("0.0.0.0", port);
    }

    void Stop()
    {
        svr.stop();
    }
};