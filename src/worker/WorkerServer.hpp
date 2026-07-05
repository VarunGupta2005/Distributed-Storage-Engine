#pragma once
#include "../common/httplib.h"
#include "IStorageBackend.hpp"
#include <string>

class WorkerServer
{
private:
    httplib::Server svr;
    IStorageBackend *storage; // Injected dependency (SOLID)

public:
    WorkerServer(IStorageBackend *database) : storage(database)
    {

        // 1. POST /chunk?hash=abc (Uploads and saves a chunk)
        svr.Post("/chunk", [this](const httplib::Request &req, httplib::Response &res)
                 {
            std::string hash = req.get_param_value("hash");
            if (hash.empty()) {
                res.status = 400;
                return;
            }

            // Convert raw request body bytes to vector
            std::vector<char> data(req.body.begin(), req.body.end());

            if (this->storage->SaveChunk(hash, data)) {
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
                res.status = 404; // Not Found
            } });

        // 3. DELETE /chunk?hash=abc (Purges a physical chunk - Called by GC!)
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
};