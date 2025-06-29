#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"

namespace duckdb {

class GigapiSchema : public SchemaCatalogEntry {
public:
    GigapiSchema(Catalog &catalog, CreateSchemaInfo &info);

    optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info) override;
};

void RegisterGigapiAttach(DatabaseInstance &instance);

} // namespace duckdb 