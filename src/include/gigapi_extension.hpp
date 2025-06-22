#pragma once

#include "duckdb.hpp"

namespace duckdb {

class GigapiExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
	std::string Version() const override;
};

struct GigapiParseData : public ParserExtensionParseData {
	unique_ptr<SQLStatement> statement;
	bool from_gigapi = true;

	unique_ptr<ParserExtensionParseData> Copy() const override {
		return make_uniq_base<ParserExtensionParseData, GigapiParseData>(statement->Copy());
	}

	string ToString() const override {
		return statement->ToString();
	}

	explicit GigapiParseData(unique_ptr<SQLStatement> statement_p) : statement(std::move(statement_p)) {
	}
};

class GigapiPlannerState : public ClientContextState {
public:
	explicit GigapiPlannerState(unique_ptr<ParserExtensionParseData> parse_data_p)
	    : parse_data(std::move(parse_data_p)) {
	}

	void QueryEnd() override {
		parse_data.reset();
	}

	unique_ptr<ParserExtensionParseData> parse_data;
};

BoundStatement gigapi_bind(ClientContext &context, Binder &binder, OperatorExtensionInfo *info, SQLStatement &statement);

struct GigapiOperatorExtension : public OperatorExtension {
	GigapiOperatorExtension() : OperatorExtension() {
		Bind = gigapi_bind;
	}
	std::string GetName() override {
		return "gigapi";
	}

	unique_ptr<LogicalExtensionOperator> Deserialize(Deserializer &deserializer) override {
		throw InternalException("GigAPI operator should not be serialized");
	}
};

} // namespace duckdb
