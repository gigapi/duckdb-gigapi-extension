#pragma once

#include "duckdb.hpp"

namespace duckdb {

struct WhereConditionResult {
    string condition;
    string table_name;
    string context;
};

struct DetailedWhereConditionResult {
    string column_name;
    string operator_type;
    string value;
    string table_name;
    string context;
};

void ExtractDetailedWhereConditionsFromExpression(
    const ParsedExpression &expr,
    vector<DetailedWhereConditionResult> &results,
    const string &context = "WHERE",
    const string &table_name = ""
);

} // namespace duckdb 