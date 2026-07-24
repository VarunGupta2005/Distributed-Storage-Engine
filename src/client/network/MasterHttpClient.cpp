#include "MasterHttpClient.hpp"
#include "../../common/httplib.h"
#include "../../common/json.hpp"
#include <stdexcept>
#include <iostream>

using json = nlohmann::json;

MasterHttpClient::MasterHttpClient(const std::string &master_url)
    : master_url_(master_url) {}

void MasterHttpClient::SetAuthToken(const std::string &token)
{
  auth_token_ = token;
}

bool MasterHttpClient::CreateUser(const std::string &email, const std::string &password)
{
  httplib::Client client(master_url_);
  json payload = {{"email", email}, {"password", password}};

  auto res = client.Post("/auth/signup", payload.dump(), "application/json");

  if (!res)
  {
    std::cerr << "[ERROR] Master Node unreachable at " << master_url_ << "\n";
    return false;
  }
  if (res->status != 201)
  {
    std::cerr << "[ERROR] Signup failed. HTTP " << res->status << ": " << res->body << "\n";
    return false;
  }
  return true;
}

std::string MasterHttpClient::Login(const std::string &email, const std::string &password)
{
  httplib::Client client(master_url_);
  json payload = {{"email", email}, {"password", password}};

  auto res = client.Post("/auth/login", payload.dump(), "application/json");

  if (!res)
  {
    std::cerr << "[ERROR] Master Node unreachable at " << master_url_ << "\n";
    return "";
  }
  if (res->status == 200)
  {
    try
    {
      auto res_json = json::parse(res->body);
      return res_json["token"].get<std::string>();
    }
    catch (const std::exception &e)
    {
      std::cerr << "[ERROR] Failed to parse login response: " << e.what() << "\n";
    }
  }
  else
  {
    std::cerr << "[ERROR] Login failed. HTTP " << res->status << ": " << res->body << "\n";
  }
  return "";
}

std::vector<ChunkRoutingPlan> MasterHttpClient::InitializeUpload(const std::vector<std::string> &chunk_hashes)
{
  httplib::Client client(master_url_);
  httplib::Headers headers = {{"Authorization", "Bearer " + auth_token_}};
  json payload = {{"hashes", chunk_hashes}};

  auto res = client.Post("/upload-init", headers, payload.dump(), "application/json");

  if (!res)
  {
    throw std::runtime_error("Master Node unreachable.");
  }
  if (res->status != 200)
  {
    throw std::runtime_error("Upload-Init failed. HTTP " + std::to_string(res->status) + ": " + res->body);
  }

  auto res_json = json::parse(res->body);
  std::vector<ChunkRoutingPlan> upload_plans;

  for (const auto &item : res_json["missing_hashes"])
  {
    ChunkRoutingPlan plan;
    plan.hash = item["hash"].get<std::string>();

    for (const auto &node_json : item["assigned_nodes"])
    {
      NodeInfo info;
      info.advertised_host = node_json["advertised_host"].get<std::string>();
      info.advertised_port = node_json["advertised_port"].get<int>();
      plan.assigned_nodes.push_back(info);
    }
    upload_plans.push_back(plan);
  }
  return upload_plans;
}

bool MasterHttpClient::CommitFile(const std::string &filename, long long file_size, const std::vector<std::string> &chunk_hashes)
{
  httplib::Client client(master_url_);
  httplib::Headers headers = {{"Authorization", "Bearer " + auth_token_}};
  json payload = {{"filename", filename}, {"file_size", file_size}, {"chunk_hashes", chunk_hashes}};

  auto res = client.Post("/files/commit", headers, payload.dump(), "application/json");

  if (!res)
  {
    std::cerr << "[ERROR] Commit failed: Master Node unreachable.\n";
    return false;
  }
  if (res->status != 200)
  {
    std::cerr << "[ERROR] Commit rejected by Master. HTTP " << res->status << ": " << res->body << "\n";
    return false;
  }
  return true;
}

FileDownloadManifest MasterHttpClient::GetDownloadPlan(const std::string &filename)
{
  httplib::Client client(master_url_);
  httplib::Headers headers = {{"Authorization", "Bearer " + auth_token_}};

  auto res = client.Get("/files/download-plan?filename=" + filename, headers);

  if (!res)
  {
    throw std::runtime_error("Master Node unreachable.");
  }
  if (res->status != 200)
  {
    throw std::runtime_error("Download-Plan failed. HTTP " + std::to_string(res->status) + ": " + res->body);
  }

  auto res_json = json::parse(res->body);
  FileDownloadManifest manifest;
  manifest.file_size = res_json["file_size"].get<long long>();

  for (const auto &item : res_json["chunks"])
  {
    ChunkRoutingPlan plan;
    plan.hash = item["hash"].get<std::string>();

    for (const auto &node_json : item["assigned_nodes"])
    {
      NodeInfo info;
      info.advertised_host = node_json["advertised_host"].get<std::string>();
      info.advertised_port = node_json["advertised_port"].get<int>();
      plan.assigned_nodes.push_back(info);
    }
    manifest.plans.push_back(plan);
  }
  return manifest;
}

bool MasterHttpClient::FileExists(const std::string &filename)
{
  httplib::Client client(master_url_);
  httplib::Headers headers = {{"Authorization", "Bearer " + auth_token_}};

  auto res = client.Get("/files/exists?filename=" + filename, headers);
  if (res && res->status == 200)
  {
    auto res_json = nlohmann::json::parse(res->body);
    return res_json["exists"].get<bool>();
  }
  return false;
}

bool MasterHttpClient::RemoveFile(const std::string &filename)
{
  httplib::Client client(master_url_);
  httplib::Headers headers = {{"Authorization", "Bearer " + auth_token_}};

  auto res = client.Delete("/files?filename=" + filename, headers);
  return res && res->status == 200;
}