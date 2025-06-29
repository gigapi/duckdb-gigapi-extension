#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"

namespace duckdb {

class GigapiSchema : public SchemaCatalogEntry {
public:
    GigapiSchema(Catalog &catalog, const std::string &name);

    TableCatalogEntry *GetTable(ClientContext &context, const std::string &table_name) override;
};

void RegisterGigapiAttach(DatabaseInstance &instance);

} // namespace duckdb 