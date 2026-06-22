#include "MasterServer.hpp"
#include "PostgresRepository.hpp"
#include <iostream>
#include <string>
#include <cstdlib>

int main(int argc, char *argv[])
{
  int port = 8081;

  if (argc > 1)
  {
    port = std::stoi(argv[1]);
  }

  const char *db_host = std::getenv("DB_HOST") ? std::getenv("DB_HOST") : "127.0.0.1";

  std::string conn_str = "user=postgres password=password host=" + std::string(db_host) + " port=5432 dbname=metadata";
  PostgresRepository repo(conn_str);
  MasterServer server(port, &repo);

  std::cout << "[INIT] Master Node booting up on port " << port << "...\n";
  std::cout << "[INIT] Connecting to PostgreSQL on " << db_host << ":5432...\n";

  server.Listen(port);
  return 0;
}