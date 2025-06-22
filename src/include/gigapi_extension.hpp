#pragma once

#include "duckdb.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/planner/operator_extension.hpp"

namespace duckdb {

struct GigapiParseData : public ParserExtensionParseData {
	unique_ptr<SQLStatement> statement;

	explicit GigapiParseData(unique_ptr<SQLStatement> statement);

	unique_ptr<ParserExtensionParseData> Copy() const override;
	string ToString() const override;
};

struct GigapiState : public ClientContextState {
	explicit GigapiState(unique_ptr<ParserExtensionParseData> parse_data);
	~GigapiState() override = default;

	unique_ptr<ParserExtensionParseData> parse_data;
};

ParserExtensionParseResult gigapi_parse(ParserExtensionInfo *, const std::string &query);
ParserExtensionPlanResult gigapi_plan(ParserExtensionInfo *, ClientContext &context,
                                      unique_ptr<ParserExtensionParseData> parse_data);
BoundStatement gigapi_bind(ClientContext &context, Binder &binder, OperatorExtensionInfo *info, SQLStatement &statement);

struct GigapiParserExtension : public ParserExtension {
	GigapiParserExtension();
};

class GigapiOperatorExtension : public OperatorExtension {
public:
	GigapiOperatorExtension();
	std::string GetName() override;
	unique_ptr<LogicalExtensionOperator> Deserialize(Deserializer &deserializer) override;
};

class GigapiExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
	std::string Version() const override;
};

} // namespace duckdb
