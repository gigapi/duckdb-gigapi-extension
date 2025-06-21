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
	string query;
	explicit GigapiParseData(string query) : query(std::move(query)) {
	}

	unique_ptr<ParserExtensionParseData> Copy() const override {
		return make_uniq<GigapiParseData>(query);
	}

	string ToString() const override {
		return query;
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
};

} // namespace duckdb
