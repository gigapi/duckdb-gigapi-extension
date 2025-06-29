#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

class GigapiSchema : public SchemaCatalogEntry {
public:
    GigapiSchema(Catalog &catalog, CreateSchemaInfo &info);

    optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info) override;
};

class GigapiStorageExtension : public StorageExtension {
public:
    std::unique_ptr<duckdb::Catalog> Attach(
        duckdb::StorageExtensionInfo *storage_info,
        duckdb::ClientContext &context,
        duckdb::AttachedDatabase &db,
        const std::string &name,
        duckdb::AttachInfo &info,
        duckdb::AccessMode access_mode
    );
};

void RegisterGigapiAttach(DatabaseInstance &instance);

} // namespace duckdb 