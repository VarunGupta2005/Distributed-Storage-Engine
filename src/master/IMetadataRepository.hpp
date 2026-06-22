#pragma once
#include <string>

class IMetadataRepository
{
public:
  virtual ~IMetadataRepository() = default;

  // verify database connectivity
  virtual std::string GetDatabaseVersion() = 0;
};