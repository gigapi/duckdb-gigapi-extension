#include "include/gigapi_attach.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include <string>
#include <vector>

namespace duckdb {

class GigapiStorageExtension : public StorageExtension {
public:
    unique_ptr<Catalog> Attach(StorageExtensionInfo *storage_info, ClientContext &context, AttachedDatabase &db, const string &name, AttachInfo &info, AccessMode access_mode) override {
        // info.path = database name
        // name = attached schema
        std::string database_name = info.path;

        // 1. Create the schema if it doesn't exist (use SQL for IF NOT EXISTS)
        std::string create_schema_sql = "CREATE SCHEMA IF NOT EXISTS " + name;
        auto schema_res = context.Query(create_schema_sql);
        if (schema_res->HasError()) {
            throw Exception(ExceptionType::CATALOG, "Failed to create schema: " + schema_res->GetError());
        }

        // 2. Enumerate tables via gigapi('SHOW TABLES')
        auto show_tables_query = "SELECT * FROM gigapi('SHOW TABLES')";
        auto result = context.Query(show_tables_query);
        if (!result || result->HasError()) {
            throw Exception(ExceptionType::CATALOG, "Failed to enumerate tables from GigAPI: " + (result ? result->GetError() : "unknown error"));
        }

        // 3. For each table, create a view in the attached schema
        for (size_t i = 0; i < result->RowCount(); ++i) {
            auto table_name_val = result->GetValue(0, i);
            if (table_name_val.IsNull()) continue;
            std::string table_name = table_name_val.ToString();
            std::string view_sql = "CREATE OR REPLACE VIEW " + name + "." + table_name +
                                   " AS SELECT * FROM gigapi('" + database_name + "." + table_name + "')";
            auto view_res = context.Query(view_sql);
            if (view_res->HasError()) {
                throw Exception(ExceptionType::CATALOG, "Failed to create view: " + view_res->GetError());
            }
        }

        // Return nullptr (no custom catalog needed, views are registered)
        return nullptr;
    }
};

void RegisterGigapiAttach(DatabaseInstance &instance) {
    // Register the storage extension for TYPE gigapi
    auto &config = DBConfig::GetConfig(instance);
    config.storage_extensions["gigapi"] = make_uniq<GigapiStorageExtension>();
}

} // namespace duckdb 