#pragma once

// Handles command-line argument parsing, environment loading,
// and signal-handling registration for the client application.
class CLI
{
public:
  // Parses argv and dispatches commands to the DSEClient orchestrator.
  static int Run(int argc, char *argv[]);
};