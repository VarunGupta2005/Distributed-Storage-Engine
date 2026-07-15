#pragma once
#include "../common/httplib.h"
#include "../common/json.hpp"
#include "../common/crypto.hpp"
#include "IMetadataRepository.hpp"
#include <string>
#include <vector>
#include <iostream>

class MasterServer
{
private:
    httplib::Server svr;
    IMetadataRepository *db;

public:
    MasterServer(int node_port, IMetadataRepository *repo) : db(repo)
    {
        // ---------------------------------------------------------
        // GET / : Health Check
        // ---------------------------------------------------------
        svr.Get("/", [node_port, this](const httplib::Request &req, httplib::Response &res)
                {
            try {
                std::string db_version = this->db->GetDatabaseVersion();
                std::string message = "Master Node OK. Port: " + std::to_string(node_port) + "\nDB: " + db_version;
                res.set_content(message, "text/plain"); 
            } catch (const std::exception &e) {
                res.status = 500;
                res.set_content(R"({"error": "Database offline"})", "application/json");
            } });

        // ---------------------------------------------------------
        // POST /auth/signup : User Registration
        // ---------------------------------------------------------
        svr.Post("/auth/signup", [this](const httplib::Request &req, httplib::Response &res)
                 {
            try {
                auto json = nlohmann::json::parse(req.body);
                std::string email = json.at("email").get<std::string>();
                std::string raw_password = json.at("password").get<std::string>();

                std::string salt = GenerateSalt();
                std::string hashed_password = HashPasswordPBKDF2(raw_password, salt);

                if (this->db->CreateUser(email, hashed_password, salt)) {
                    res.status = 201; 
                    res.set_content(R"({"status": "success"})", "application/json");
                } else {
                    res.status = 409; // Conflict (email likely exists)
                    res.set_content(R"({"error": "Registration failed"})", "application/json");
                }
            } 
            catch (const nlohmann::json::exception &e) { 
                res.status = 400; 
                res.set_content(R"({"error": "Invalid JSON payload"})", "application/json");
            }
            catch (const std::exception &e) {
                res.status = 500;
                res.set_content(R"({"error": "Internal server error"})", "application/json");
            } });

        // ---------------------------------------------------------
        // POST /auth/login : Authentication
        // ---------------------------------------------------------
        svr.Post("/auth/login", [this](const httplib::Request &req, httplib::Response &res)
                 {
            try {
                auto json = nlohmann::json::parse(req.body);
                std::string email = json.at("email").get<std::string>();
                std::string password = json.at("password").get<std::string>();

                UserRecord user = this->db->GetUserByEmail(email);

                // Static dummy salt to normalize computation time for nonexistent users.
                static const std::string dummy_salt = "00000000000000000000000000000000";

                if (user.found) {
                    std::string attempt_hash = HashPasswordPBKDF2(password, user.salt);

                    // Constant-time memory comparison to mitigate cryptographic timing leaks.
                    if (ConstantTimeEquals(attempt_hash, user.password_hash)) {
                        std::string token = GenerateSessionToken(user.id, "my_shared_secret_key");
                        nlohmann::json response = {{"status", "success"}, {"token", token}};
                        res.set_content(response.dump(), "application/json");
                        return; 
                    }
                } else {
                    // Computationally equivalent hash operation to mask the database miss.
                    HashPasswordPBKDF2(password, dummy_salt);
                }
                
                res.status = 401; 
                res.set_content(R"({"error": "Invalid credentials"})", "application/json");
            } 
            catch (const nlohmann::json::exception &e) { 
                res.status = 400; 
            }
            catch (const std::exception &e) {
                res.status = 500;
            } });

        // ---------------------------------------------------------
        // POST /upload-init : Deduplication & Lease Renewal
        // ---------------------------------------------------------
        svr.Post("/upload-init", [this](const httplib::Request &req, httplib::Response &res)
                 {
            try {
                auto json = nlohmann::json::parse(req.body);
                std::vector<std::string> client_hashes = json.at("hashes").get<std::vector<std::string>>();

                std::vector<std::string> missing = this->db->FilterMissingChunks(client_hashes);

                nlohmann::json response = {
                    {"status", "success"},
                    {"missing_hashes", missing}
                };
                res.set_content(response.dump(), "application/json");
                
            } 
            catch (const nlohmann::json::exception &e) { 
                res.status = 400; 
            }
            catch (const std::runtime_error& e) {
                if (std::string(e.what()) == "CHUNK_LOCKED") {
                    res.status = 423; // HTTP 423 Locked (Tombstone state)
                    res.set_content(R"({"error": "Resource locked. Retry."})", "application/json");
                } else {
                    res.status = 500;
                }
            } 
            catch (const std::exception &e) {
                res.status = 500;
            } });

        // ---------------------------------------------------------
        // POST /files/commit : Finalize Two-Phase Commit
        // ---------------------------------------------------------
        svr.Post("/files/commit", [this](const httplib::Request &req, httplib::Response &res)
                 {
            try {
                auto json = nlohmann::json::parse(req.body);
                std::string filename = json.at("filename").get<std::string>();
                long long file_size = json.at("file_size").get<long long>();
                std::vector<std::string> chunk_hashes = json.at("chunk_hashes").get<std::vector<std::string>>();

                this->db->CommitFile(filename, file_size, chunk_hashes);

                res.status = 200;
                res.set_content(R"({"status": "success"})", "application/json");
            } 
            catch (const nlohmann::json::exception &e) { 
                res.status = 400; 
            }
            catch (const std::exception &e) {
                res.status = 500;
            } });

        // ---------------------------------------------------------
        // POST /heartbeat : Worker Node Auto-Discovery
        // ---------------------------------------------------------
        svr.Post("/heartbeat", [this](const httplib::Request &req, httplib::Response &res)
                 {
            if (!req.has_header("X-Cluster-Secret") || 
                 req.get_header_value("X-Cluster-Secret") != "my_shared_secret_key") 
            {
                res.status = 401;
                return;
            }

            try {
                auto json = nlohmann::json::parse(req.body);
                
                this->db->RegisterWorker(
                    json.at("internal_host").get<std::string>(), 
                    json.at("internal_port").get<int>(),
                    json.at("advertised_host").get<std::string>(), 
                    json.at("advertised_port").get<int>(),
                    json.at("free_space").get<long long>()
                );

                res.status = 200;
            } 
            catch (const nlohmann::json::exception &e) { 
                res.status = 400; 
            }
            catch (const std::exception &e) {
                res.status = 500;
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