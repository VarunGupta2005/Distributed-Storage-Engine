#pragma once
#include "../common/httplib.h"
#include "../common/json.hpp"
#include "../common/crypto.hpp"
#include "IMetadataRepository.hpp"
#include <string>
#include <vector>
#include <iostream>
#include <cstdlib>

class MasterServer
{
private:
    httplib::Server svr;
    IMetadataRepository *db;

    // Configuration values loaded dynamically from environment variables
    std::string cluster_secret;
    std::string session_secret;

    // Registers lambda bindings that delegate requests to dedicated handler functions
    void RegisterRoutes(int node_port)
    {
        svr.Get("/", [this, node_port](const httplib::Request &req, httplib::Response &res)
                { HandleHealthCheck(req, res, node_port); });

        svr.Post("/auth/signup", [this](const httplib::Request &req, httplib::Response &res)
                 { HandleSignup(req, res); });

        svr.Post("/auth/login", [this](const httplib::Request &req, httplib::Response &res)
                 { HandleLogin(req, res); });

        svr.Post("/upload-init", [this](const httplib::Request &req, httplib::Response &res)
                 { HandleUploadInit(req, res); });

        svr.Post("/files/commit", [this](const httplib::Request &req, httplib::Response &res)
                 { HandleCommit(req, res); });

        svr.Post("/heartbeat", [this](const httplib::Request &req, httplib::Response &res)
                 { HandleHeartbeat(req, res); });

        svr.Post("/admin/cluster/add", [this](const httplib::Request &req, httplib::Response &res)
                 { HandleAddCluster(req, res); });

        svr.Post("/admin/cluster/replace", [this](const httplib::Request &req, httplib::Response &res)
                 { HandleReplaceCluster(req, res); });
    }

    // --- ENDPOINT HANDLERS ---

    void HandleHealthCheck(const httplib::Request &req, httplib::Response &res, int node_port)
    {
        try
        {
            std::string db_version = this->db->GetDatabaseVersion();
            std::string message = "Master Node OK. Port: " + std::to_string(node_port) + "\nDB: " + db_version;
            res.set_content(message, "text/plain");
        }
        catch (const std::exception &e)
        {
            res.status = 500;
            res.set_content(R"({"error": "Database offline"})", "application/json");
        }
    }

    void HandleSignup(const httplib::Request &req, httplib::Response &res)
    {
        try
        {
            auto json = nlohmann::json::parse(req.body);
            std::string email = json.at("email").get<std::string>();
            std::string raw_password = json.at("password").get<std::string>();

            std::string salt = GenerateSalt();
            std::string hashed_password = HashPasswordPBKDF2(raw_password, salt);

            if (this->db->CreateUser(email, hashed_password, salt))
            {
                res.status = 201;
                res.set_content(R"({"status": "success"})", "application/json");
            }
            else
            {
                res.status = 409;
                res.set_content(R"({"error": "Registration failed"})", "application/json");
            }
        }
        catch (const nlohmann::json::exception &e)
        {
            res.status = 400;
            res.set_content(R"({"error": "Invalid JSON payload"})", "application/json");
        }
        catch (const std::exception &e)
        {
            res.status = 500;
            res.set_content(R"({"error": "Internal server error"})", "application/json");
        }
    }

    void HandleLogin(const httplib::Request &req, httplib::Response &res)
    {
        try
        {
            auto json = nlohmann::json::parse(req.body);
            std::string email = json.at("email").get<std::string>();
            std::string password = json.at("password").get<std::string>();

            UserRecord user = this->db->GetUserByEmail(email);
            static const std::string dummy_salt = "00000000000000000000000000000000";

            if (user.found)
            {
                std::string attempt_hash = HashPasswordPBKDF2(password, user.salt);

                if (ConstantTimeEquals(attempt_hash, user.password_hash))
                {
                    std::string token = GenerateSessionToken(user.id, session_secret);
                    nlohmann::json response = {{"status", "success"}, {"token", token}};
                    res.set_content(response.dump(), "application/json");
                    return;
                }
            }
            else
            {
                HashPasswordPBKDF2(password, dummy_salt);
            }

            res.status = 401;
            res.set_content(R"({"error": "Invalid credentials"})", "application/json");
        }
        catch (const nlohmann::json::exception &e)
        {
            res.status = 400;
        }
        catch (const std::exception &e)
        {
            res.status = 500;
        }
    }

    void HandleUploadInit(const httplib::Request &req, httplib::Response &res)
    {
        try
        {
            auto json = nlohmann::json::parse(req.body);
            std::vector<std::string> client_hashes = json.at("hashes").get<std::vector<std::string>>();

            std::vector<MissingChunkPlan> missing = this->db->FilterMissingChunks(client_hashes);

            nlohmann::json missing_json = nlohmann::json::array();
            for (const auto &plan : missing)
            {
                nlohmann::json nodes_json = nlohmann::json::array();
                for (const auto &node : plan.assigned_nodes)
                {
                    nodes_json.push_back({{"advertised_host", node.advertised_host},
                                          {"advertised_port", node.advertised_port}});
                }
                missing_json.push_back({{"hash", plan.hash},
                                        {"assigned_nodes", nodes_json}});
            }
            nlohmann::json response = {
                {"status", "success"},
                {"missing_hashes", missing_json}};
            res.set_content(response.dump(), "application/json");
        }
        catch (const nlohmann::json::exception &e)
        {
            res.status = 400;
        }
        catch (const std::runtime_error &e)
        {
            if (std::string(e.what()) == "CHUNK_LOCKED")
            {
                res.status = 423;
                res.set_content(R"({"error": "Resource locked. Retry."})", "application/json");
            }
            else
            {
                res.status = 500;
            }
        }
        catch (const std::exception &e)
        {
            res.status = 500;
        }
    }

    void HandleCommit(const httplib::Request &req, httplib::Response &res)
    {
        try
        {
            auto json = nlohmann::json::parse(req.body);
            std::string filename = json.at("filename").get<std::string>();
            long long file_size = json.at("file_size").get<long long>();
            std::vector<std::string> chunk_hashes = json.at("chunk_hashes").get<std::vector<std::string>>();

            this->db->CommitFile(filename, file_size, chunk_hashes);

            res.status = 200;
            res.set_content(R"({"status": "success"})", "application/json");
        }
        catch (const nlohmann::json::exception &e)
        {
            res.status = 400;
        }
        catch (const std::exception &e)
        {
            res.status = 500;
        }
    }

    void HandleHeartbeat(const httplib::Request &req, httplib::Response &res)
    {
        if (!req.has_header("X-Cluster-Secret") ||
            req.get_header_value("X-Cluster-Secret") != cluster_secret)
        {
            res.status = 401;
            return;
        }

        try
        {
            auto json = nlohmann::json::parse(req.body);

            this->db->RegisterWorker(
                json.at("internal_host").get<std::string>(),
                json.at("internal_port").get<int>(),
                json.at("advertised_host").get<std::string>(),
                json.at("advertised_port").get<int>(),
                json.at("free_space").get<long long>());

            res.status = 200;
        }
        catch (const nlohmann::json::exception &e)
        {
            res.status = 400;
        }
        catch (const std::exception &e)
        {
            res.status = 500;
        }
    }

    void HandleAddCluster(const httplib::Request &req, httplib::Response &res)
    {
        try
        {
            auto json = nlohmann::json::parse(req.body);
            std::string cluster_name = json.at("cluster_name").get<std::string>();
            std::vector<int> active_node_ids = json.at("active_node_ids").get<std::vector<int>>();

            this->db->AddCluster(cluster_name, active_node_ids);

            res.status = 200;
            res.set_content(R"({"status": "Load distributed successfully."})", "application/json");
        }
        catch (const std::exception &e)
        {
            res.status = 500;
            res.set_content(R"({"error": "Failed to integrate cluster"})", "application/json");
        }
    }

    void HandleReplaceCluster(const httplib::Request &req, httplib::Response &res)
    {
        try
        {
            auto json = nlohmann::json::parse(req.body);
            int cluster_id = json.at("cluster_id").get<int>();
            std::vector<int> new_worker_ids = json.at("new_worker_ids").get<std::vector<int>>();

            this->db->ReplaceCluster(cluster_id, new_worker_ids);

            nlohmann::json response = {
                {"status", "success"},
                {"message", "Cluster nodes replaced in-place. Former nodes marked READ_ONLY."}};

            res.status = 200;
            res.set_content(response.dump(), "application/json");
        }
        catch (const std::exception &e)
        {
            res.status = 500;
            res.set_content(R"({"error": "Failed to execute in-place cluster replacement"})", "application/json");
        }
    }

public:
    // Constructor handles initialization, configuration loading, and route registration delegation
    MasterServer(int node_port, IMetadataRepository *repo, const std::string &cluster_secret, const std::string &session_secret) : db(repo), cluster_secret(cluster_secret), session_secret(session_secret)
    {
        RegisterRoutes(node_port);
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