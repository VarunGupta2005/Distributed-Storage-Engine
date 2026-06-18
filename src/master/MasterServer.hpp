#pragma once

// Include the header-only HTTP library
#include "../common/httplib.h"
#include <string>

class MasterServer
{
private:
  httplib::Server svr;

public:
  // Constructor: Define your API endpoints here
  MasterServer(int node_port)
  {

    // A simple GET request to the root URL "/"
    svr.Get("/", [node_port](const httplib::Request &req, httplib::Response &res)
            {
            std::string message = "Hello World! Answered by Master Node on port " + std::to_string(node_port) + "\n";
            res.set_content(message, "text/plain"); });
  }

  // Start the server blocking loop
  void Listen(int port)
  {
    // 0.0.0.0 allows NGINX (from Docker) to reach this process
    svr.listen("0.0.0.0", port);
  }
};