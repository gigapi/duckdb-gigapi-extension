#include "parse_where.hpp"
#include "duckdb.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/between_expression.hpp"
#include "duckdb/parser/expression/lambda_expression.hpp"
#include "duckdb/parser/expression/positional_reference_expression.hpp"
#include "duckdb/parser/expression/parameter_expression.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/main/extension_util.hpp"

namespace duckdb {

struct ParseWhereState : public GlobalTableFunctionState {
    idx_t row = 0;
    vector<WhereConditionResult> results;
};

struct ParseWhereBindData : public TableFunctionData {
    string sql;
};

static unique_ptr<FunctionData> ParseWhereBind(ClientContext &context, 
                                    TableFunctionBindInput &input, 
                                    vector<LogicalType> &return_types, 
                                    vector<string> &names) {
    string sql_input = StringValue::Get(input.inputs[0]);
    
    return_types = {
        LogicalType::VARCHAR,  // condition
        LogicalType::VARCHAR,  // table_name
        LogicalType::VARCHAR   // context
    };
    
    names = {"condition", "table_name", "context"};
    
    auto result = make_uniq<ParseWhereBindData>();
    result->sql = sql_input;

    return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> ParseWhereInit(ClientContext &context,
    TableFunctionInitInput &input) {
    return make_uniq<ParseWhereState>();
}

static string ExpressionToString(const ParsedExpression &expr) {
    return expr.ToString();
}

static void ExtractWhereConditionsFromExpression(
    const ParsedExpression &expr,
    vector<WhereConditionResult> &results,
    const string &context = "WHERE",
    const string &table_name = ""
) {
    if (expr.type == ExpressionType::INVALID) return;

    switch (expr.GetExpressionClass()) {
        case ExpressionClass::CONJUNCTION: {
            auto &conj = (ConjunctionExpression &)expr;
            for (auto &child : conj.children) {
                ExtractWhereConditionsFromExpression(*child, results, context, table_name);
            }
            break;
        }
        case ExpressionClass::COMPARISON: {
            auto &comp = (ComparisonExpression &)expr;
            results.push_back(WhereConditionResult{
                ExpressionToString(comp),
                table_name,
                context
            });
            break;
        }
        case ExpressionClass::OPERATOR: {
            auto &op = (OperatorExpression &)expr;
            results.push_back(WhereConditionResult{
                ExpressionToString(op),
                table_name,
                context
            });
            break;
        }
        case ExpressionClass::FUNCTION: {
            auto &func = (FunctionExpression &)expr;
            results.push_back(WhereConditionResult{
                ExpressionToString(func),
                table_name,
                context
            });
            break;
        }
        case ExpressionClass::BETWEEN: {
            auto &between = (BetweenExpression &)expr;
            results.push_back(WhereConditionResult{
                ExpressionToString(between),
                table_name,
                context
            });
            break;
        }
        case ExpressionClass::CASE: {
            auto &case_expr = (CaseExpression &)expr;
            results.push_back(WhereConditionResult{
                ExpressionToString(case_expr),
                table_name,
                context
            });
            break;
        }
        default:
            break;
    }
}

static void ExtractWhereConditionsFromQueryNode(
    const QueryNode &node,
    vector<WhereConditionResult> &results
) {
    if (node.type == QueryNodeType::SELECT_NODE) {
        auto &select_node = (SelectNode &)node;
        string table_name = "(empty)";  // Default table name

        // Extract table name from FROM clause
        if (select_node.from_table) {
            if (select_node.from_table->type == TableReferenceType::BASE_TABLE) {
                auto &base = (BaseTableRef &)*select_node.from_table;
                table_name = base.table_name;
            }
        }

        // Extract WHERE conditions
        if (select_node.where_clause) {
            ExtractWhereConditionsFromExpression(*select_node.where_clause, results, "WHERE", table_name);
        }

        // Extract HAVING conditions
        if (select_node.having) {
            ExtractWhereConditionsFromExpression(*select_node.having, results, "HAVING", table_name);
        }
    }
}

static void ExtractWhereConditionsFromSQL(const string &sql, vector<WhereConditionResult> &results) {
    Parser parser;

    try {
        parser.ParseQuery(sql);
    } catch (const ParserException &ex) {
        return;
    }

    for (auto &stmt : parser.statements) {
        if (stmt->type == StatementType::SELECT_STATEMENT) {
            auto &select_stmt = (SelectStatement &)*stmt;
            if (select_stmt.node) {
                ExtractWhereConditionsFromQueryNode(*select_stmt.node, results);
            }
        }
    }
}

static void ParseWhereFunction(ClientContext &context,
                   TableFunctionInput &data,
                   DataChunk &output) {
    auto &state = (ParseWhereState &)*data.global_state;
    auto &bind_data = (ParseWhereBindData &)*data.bind_data;

    if (state.results.empty() && state.row == 0) {
        ExtractWhereConditionsFromSQL(bind_data.sql, state.results);
    }

    if (state.row >= state.results.size()) {
        return;
    }

    auto &result = state.results[state.row];
    output.SetCardinality(1);
    output.SetValue(0, 0, Value(result.condition));
    output.SetValue(1, 0, Value(result.table_name));
    output.SetValue(2, 0, Value(result.context));

    state.row++;
}

static void ParseWhereScalarFunction(DataChunk &args, ExpressionState &state, Vector &result) {
    UnaryExecutor::Execute<string_t, list_entry_t>(args.data[0], result, args.size(),
    [&result](string_t query) -> list_entry_t {
        auto query_string = query.GetString();
        vector<WhereConditionResult> conditions;
        ExtractWhereConditionsFromSQL(query_string, conditions);

        auto current_size = ListVector::GetListSize(result);
        auto number_of_conditions = conditions.size();
        auto new_size = current_size + number_of_conditions;

        if (ListVector::GetListCapacity(result) < new_size) {
            ListVector::Reserve(result, new_size);
        }

        auto &struct_vector = ListVector::GetEntry(result);
        auto &entries = StructVector::GetEntries(struct_vector);
        auto &condition_entry = *entries[0];
        auto &table_entry = *entries[1];
        auto &context_entry = *entries[2];

        auto condition_data = FlatVector::GetData<string_t>(condition_entry);
        auto table_data = FlatVector::GetData<string_t>(table_entry);
        auto context_data = FlatVector::GetData<string_t>(context_entry);

        for (size_t i = 0; i < number_of_conditions; i++) {
            const auto &condition = conditions[i];
            auto idx = current_size + i;

            condition_data[idx] = StringVector::AddStringOrBlob(condition_entry, condition.condition);
            table_data[idx] = StringVector::AddStringOrBlob(table_entry, condition.table_name);
            context_data[idx] = StringVector::AddStringOrBlob(context_entry, condition.context);
        }

        ListVector::SetListSize(result, new_size);
        return list_entry_t(current_size, number_of_conditions);
    });
}

void RegisterParseWhereFunction(DatabaseInstance &db) {
    TableFunction tf("parse_where", {LogicalType::VARCHAR}, ParseWhereFunction, ParseWhereBind, ParseWhereInit);
    ExtensionUtil::RegisterFunction(db, tf);
}

void RegisterParseWhereScalarFunction(DatabaseInstance &db) {
    auto return_type = LogicalType::LIST(LogicalType::STRUCT({
        {"condition", LogicalType::VARCHAR},
        {"table_name", LogicalType::VARCHAR},
        {"context", LogicalType::VARCHAR}
    }));
    ScalarFunction sf("parse_where", {LogicalType::VARCHAR}, return_type, ParseWhereScalarFunction);
    ExtensionUtil::RegisterFunction(db, sf);
}

static string DetailedExpressionTypeToOperator(ExpressionType type) {
    switch (type) {
        case ExpressionType::COMPARE_EQUAL:
            return "=";
        case ExpressionType::COMPARE_NOTEQUAL:
            return "!=";
        case ExpressionType::COMPARE_LESSTHAN:
            return "<";
        case ExpressionType::COMPARE_GREATERTHAN:
            return ">";
        case ExpressionType::COMPARE_LESSTHANOREQUALTO:
            return "<=";
        case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
            return ">=";
        case ExpressionType::COMPARE_DISTINCT_FROM:
            return "IS DISTINCT FROM";
        case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
            return "IS NOT DISTINCT FROM";
        default:
            return "UNKNOWN";
    }
}

void ExtractDetailedWhereConditionsFromExpression(
    const ParsedExpression &expr,
    vector<DetailedWhereConditionResult> &results,
    const string &context,
    const string &table_name
) {
    if (expr.type == ExpressionType::INVALID) return;

    switch (expr.GetExpressionClass()) {
        case ExpressionClass::CONJUNCTION: {
            auto &conj = (ConjunctionExpression &)expr;
            for (auto &child : conj.children) {
                ExtractDetailedWhereConditionsFromExpression(*child, results, context, table_name);
            }
            break;
        }
        case ExpressionClass::COMPARISON: {
            auto &comp = (ComparisonExpression &)expr;
            DetailedWhereConditionResult result;
            result.context = context;
            result.table_name = table_name;
            
            // Extract column name
            if (comp.left->GetExpressionClass() == ExpressionClass::COLUMN_REF) {
                auto &col_ref = (ColumnRefExpression &)*comp.left;
                result.column_name = col_ref.GetColumnName();
            }
            
            // Extract operator
            result.operator_type = DetailedExpressionTypeToOperator(comp.type);
            
            // Extract value
            if (comp.right->GetExpressionClass() == ExpressionClass::CONSTANT) {
                auto &const_expr = (ConstantExpression &)*comp.right;
                result.value = const_expr.value.ToString();
            } else {
                result.value = comp.right->ToString();
            }
            
            results.push_back(result);
            break;
        }
        case ExpressionClass::BETWEEN: {
            auto &between = (BetweenExpression &)expr;
            DetailedWhereConditionResult result;
            result.context = context;
            result.table_name = table_name;
            
            // Extract column name
            if (between.input->GetExpressionClass() == ExpressionClass::COLUMN_REF) {
                auto &col_ref = (ColumnRefExpression &)*between.input;
                result.column_name = col_ref.GetColumnName();
            }
            
            // For BETWEEN, we'll create two conditions: >= lower AND <= upper
            result.operator_type = ">=";
            if (between.lower->GetExpressionClass() == ExpressionClass::CONSTANT) {
                auto &const_expr = (ConstantExpression &)*between.lower;
                result.value = const_expr.value.ToString();
            } else {
                result.value = between.lower->ToString();
            }
            results.push_back(result);
            
            // Add the upper bound condition
            DetailedWhereConditionResult upper_result = result;
            upper_result.operator_type = "<=";
            if (between.upper->GetExpressionClass() == ExpressionClass::CONSTANT) {
                auto &const_expr = (ConstantExpression &)*between.upper;
                upper_result.value = const_expr.value.ToString();
            } else {
                upper_result.value = between.upper->ToString();
            }
            results.push_back(upper_result);
            break;
        }
        case ExpressionClass::OPERATOR: {
            auto &op = (OperatorExpression &)expr;
            if (op.children.size() >= 2) {
                DetailedWhereConditionResult result;
                result.context = context;
                result.table_name = table_name;
                
                // Extract column name
                if (op.children[0]->GetExpressionClass() == ExpressionClass::COLUMN_REF) {
                    auto &col_ref = (ColumnRefExpression &)*op.children[0];
                    result.column_name = col_ref.GetColumnName();
                }
                
                // Extract operator
                result.operator_type = DetailedExpressionTypeToOperator(op.type);
                
                // Extract value
                if (op.children[1]->GetExpressionClass() == ExpressionClass::CONSTANT) {
                    auto &const_expr = (ConstantExpression &)*op.children[1];
                    result.value = const_expr.value.ToString();
                } else {
                    result.value = op.children[1]->ToString();
                }
                
                results.push_back(result);
            }
            break;
        }
        default:
            break;
    }
}

struct ParseWhereDetailedState : public GlobalTableFunctionState {
    idx_t row = 0;
    vector<DetailedWhereConditionResult> results;
};

struct ParseWhereDetailedBindData : public TableFunctionData {
    string sql;
};

static unique_ptr<FunctionData> ParseWhereDetailedBind(ClientContext &context, 
                                    TableFunctionBindInput &input, 
                                    vector<LogicalType> &return_types, 
                                    vector<string> &names) {
    string sql_input = StringValue::Get(input.inputs[0]);
    
    return_types = {
        LogicalType::VARCHAR,  // column_name
        LogicalType::VARCHAR,  // operator_type
        LogicalType::VARCHAR,  // value
        LogicalType::VARCHAR,  // table_name
        LogicalType::VARCHAR   // context
    };
    
    names = {"column_name", "operator_type", "value", "table_name", "context"};
    
    auto result = make_uniq<ParseWhereDetailedBindData>();
    result->sql = sql_input;

    return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> ParseWhereDetailedInit(ClientContext &context,
    TableFunctionInitInput &input) {
    return make_uniq<ParseWhereDetailedState>();
}

static void ParseWhereDetailedFunction(ClientContext &context,
                   TableFunctionInput &data,
                   DataChunk &output) {
    auto &state = (ParseWhereDetailedState &)*data.global_state;
    auto &bind_data = (ParseWhereDetailedBindData &)*data.bind_data;

    if (state.results.empty() && state.row == 0) {
        Parser parser;
        try {
            parser.ParseQuery(bind_data.sql);
        } catch (const ParserException &ex) {
            return;
        }

        for (auto &stmt : parser.statements) {
            if (stmt->type == StatementType::SELECT_STATEMENT) {
                auto &select_stmt = (SelectStatement &)*stmt;
                if (select_stmt.node) {
                    if (select_stmt.node->type == QueryNodeType::SELECT_NODE) {
                        auto &select_node = (SelectNode &)*select_stmt.node;
                        string table_name = "(empty)";  // Default table name
                        
                        // Try to extract table name from FROM clause
                        if (select_node.from_table) {
                            if (select_node.from_table->type == TableReferenceType::BASE_TABLE) {
                                auto &base_table = (BaseTableRef &)*select_node.from_table;
                                table_name = base_table.table_name;
                            }
                        }
                        
                        if (select_node.where_clause) {
                            ExtractDetailedWhereConditionsFromExpression(*select_node.where_clause, state.results, "WHERE", table_name);
                        }
                        if (select_node.having) {
                            ExtractDetailedWhereConditionsFromExpression(*select_node.having, state.results, "HAVING", table_name);
                        }
                    }
                }
            }
        }
    }

    if (state.row >= state.results.size()) {
        return;
    }

    auto &result = state.results[state.row];
    output.SetCardinality(1);
    output.SetValue(0, 0, Value(result.column_name));
    output.SetValue(1, 0, Value(result.operator_type));
    output.SetValue(2, 0, Value(result.value));
    output.SetValue(3, 0, Value(result.table_name));
    output.SetValue(4, 0, Value(result.context));

    state.row++;
}

void RegisterParseWhereDetailedFunction(DatabaseInstance &db) {
    TableFunction tf("parse_where_detailed", {LogicalType::VARCHAR}, ParseWhereDetailedFunction, ParseWhereDetailedBind, ParseWhereDetailedInit);
    ExtensionUtil::RegisterFunction(db, tf);
}

} // namespace duckdb 