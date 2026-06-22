#pragma once
#include "IMetadataRepository.hpp"
#include <pqxx/pqxx>
#include <string>
#include <exception>

class PostgresRepository : public IMetadataRepository
{
private:
  std::string connection_string;

public:
  PostgresRepository(const std::string &conn_str) : connection_string(conn_str) {}

  std::string GetDatabaseVersion() override
  {
    try
    {
      // 1. Establish connection to PostgreSQL
      pqxx::connection conn(connection_string);
      if (!conn.is_open())
      {
        return "Failed to open database connection.";
      }

      // 2. Start a transaction
      pqxx::work txn(conn);

      pqxx::result res = txn.exec("SELECT version();");

      std::string version = res[0][0].as<std::string>();

      txn.commit();

      return version;
    }
    catch (const std::exception &e)
    {
      return "Database Connection Error: " + std::string(e.what());
    }
  }
};