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
#include "duckdb/common/make_uniq.hpp"

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
static void GigapiAttachHandler(ClientContext &context, const std::string &schema_name) {
    auto &catalog = Catalog::GetSystemCatalog(context);
    CreateSchemaInfo info;
    info.schema = schema_name;
    info.if_not_exists = true;
    catalog.CreateSchema(context, info);

    // Replace the schema with our custom schema
    auto &db = DatabaseInstance::Get(context);
    auto &schemas = db.GetCatalogSet(CatalogType::SCHEMA_ENTRY);
    schemas.DropEntry(context, schema_name, false);
    auto gigapi_schema = make_uniq<GigapiSchema>(catalog, info);
    schemas.AddEntry(context, std::move(gigapi_schema));
}

void RegisterGigapiAttach(DatabaseInstance &instance) {
    // Register the attach handler for TYPE gigapi
    ExtensionUtil::RegisterAttachHandler(instance, "gigapi", GigapiAttachHandler);
}

} // namespace duckdb 