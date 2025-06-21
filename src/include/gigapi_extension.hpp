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
	explicit GigapiParseData(unique_ptr<SQLStatement> stmt) : statement(std::move(stmt)) {
	}

	unique_ptr<ParserExtensionParseData> Copy() const override {
		return make_uniq<GigapiParseData>(statement->Copy());
	}

	string ToString() const override {
		return statement->ToString();
	}
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
