#include "include/gigapi_attach.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/storage_extension.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include <string>
#include <vector>

namespace duckdb {

class GigapiStorageExtension : public StorageExtension {
public:
    unique_ptr<Catalog> Attach(StorageExtensionInfo *storage_info, ClientContext &context, AttachedDatabase &db, const string &schema_name, AttachInfo &info, AccessMode access_mode) override {
        // info.path = database name
        // schema_name = attached schema
        std::string database_name = info.path;

        // 1. Create the schema if it doesn't exist
        auto &catalog = Catalog::GetSystemCatalog(context);
        CreateSchemaInfo schema_info;
        schema_info.schema = schema_name;
        schema_info.if_not_exists = true;
        catalog.CreateSchema(context, schema_info);

        // 2. Enumerate tables via gigapi('SHOW TABLES')
        Connection con(context.db);
        std::string show_tables_query = "SELECT * FROM gigapi('SHOW TABLES')";
        auto result = con.Query(show_tables_query);
        if (!result || result->HasError()) {
            throw Exception(ExceptionType::CATALOG, "Failed to enumerate tables from GigAPI: " + (result ? result->GetError() : "unknown error"));
        }

        // 3. For each table, create a view in the attached schema
        for (size_t i = 0; i < result->RowCount(); ++i) {
            auto table_name_val = result->GetValue(0, i);
            if (table_name_val.IsNull()) continue;
            std::string table_name = table_name_val.ToString();
            auto view_info = make_uniq<CreateViewInfo>();
            view_info->schema = schema_name;
            view_info->view_name = table_name;
            view_info->sql = "SELECT * FROM gigapi('" + database_name + "." + table_name + "')";
            view_info->aliases = {};
            view_info->temporary = false;
            view_info->if_not_exists = true;
            catalog.CreateView(context, *view_info);
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