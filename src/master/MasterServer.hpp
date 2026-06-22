#pragma once
#include "../common/httplib.h"
#include "IMetadataRepository.hpp"
#include <string>

class MasterServer
{
private:
  httplib::Server svr;
  IMetadataRepository *db;

public:
  MasterServer(int node_port, IMetadataRepository *repo) : db(repo)
  {
    svr.Get("/", [node_port, this](const httplib::Request &req, httplib::Response &res)
            {
        
        std::string db_version = this->db->GetDatabaseVersion();

        std::string message = "Hello World! Answered by Master Node on port " + std::to_string(node_port) + "\n";
        message += "Database Status: " + db_version + "\n";

        res.set_content(message, "text/plain"); });
  }

  void Listen(int port)
  {
    svr.listen("0.0.0.0", port);
  }
};