#include "MasterServer.hpp"
#include <iostream>
#include <string>

int main(int argc, char *argv[])
{
  // 1. Set the default port to 8081
  int port = 8081;

  // 2. If a port is passed in the terminal, use that instead
  if (argc > 1)
  {
    port = std::stoi(argv[1]);
  }

  // 3. Initialize the server class
  MasterServer server(port);

  // 4. Print to the terminal so we know it's running
  std::cout << "[INIT] Master Node booting up on port " << port << "...\n";
  std::cout << "[INIT] Waiting for NGINX to route traffic...\n";

  // 5. Start listening (This will block the program from exiting)
  server.Listen(port);

  return 0;
}