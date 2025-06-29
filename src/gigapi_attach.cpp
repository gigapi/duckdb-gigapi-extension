#include "include/gigapi_attach.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include <string>
#include <vector>

namespace duckdb {

// Table entry that pipes through gigapi()
class GigapiTableEntry : public TableCatalogEntry {
public:
    GigapiTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, const std::string &name)
        : TableCatalogEntry(CatalogType::TABLE_ENTRY, schema, name) {}

    unique_ptr<LogicalOperator> GetScanOperator(ClientContext &context) override {
        // Build: SELECT * FROM gigapi('schema.table')
        std::string qualified = schema.name + "." + name;
        std::string query = "SELECT * FROM gigapi('" + qualified + "')";
        Parser parser;
        parser.ParseQuery(query);
        if (parser.statements.empty() || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
            throw BinderException("Failed to parse gigapi table scan query");
        }
        Binder binder(context);
        auto bound = binder.Bind(*parser.statements[0]);
        return std::move(bound.plan);
    }
};

GigapiSchema::GigapiSchema(Catalog &catalog, CreateSchemaInfo &info)
    : SchemaCatalogEntry(catalog, info) {}

optional_ptr<CatalogEntry> GigapiSchema::LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info) {
    // Only handle table lookups
    if (lookup_info.type != CatalogType::TABLE_ENTRY) {
        return nullptr;
    }
    // Always return a GigapiTableEntry for any table name
    auto entry = make_uniq<GigapiTableEntry>(*catalog, *this, lookup_info.name);
    return entry.release();
}

// Handler for ATTACH ... (TYPE gigapi, ...)
static void GigapiAttachHandler(ClientContext &context, const std::string &schema_name, const std::string &database_name) {
    // 1. Create the schema if it doesn't exist
    auto &catalog = Catalog::GetSystemCatalog(context);
    CreateSchemaInfo schema_info;
    schema_info.schema = schema_name;
    schema_info.if_not_exists = true;
    catalog.CreateSchema(context, schema_info);

    // 2. Use a DuckDB connection to enumerate tables via gigapi('SHOW TABLES')
    Connection con(context.db);
    std::string show_tables_query = "SELECT * FROM gigapi('SHOW TABLES')";
    auto result = con.Query(show_tables_query);
    if (!result || result->HasError()) {
        throw Exception("Failed to enumerate tables from GigAPI: " + (result ? result->GetError() : "unknown error"));
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
}

void RegisterGigapiAttach(DatabaseInstance &instance) {
    // Register the attach handler for TYPE gigapi
    // The handler must accept (ClientContext&, schema_name, database_name)
    ExtensionUtil::RegisterAttachHandler(instance, "gigapi", GigapiAttachHandler);
}

} // namespace duckdb 