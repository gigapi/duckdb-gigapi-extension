#define DUCKDB_EXTENSION_MAIN

#include "gigapi_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"


#include "gigapi_secret.hpp"
#include "parse_where.hpp"

#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/planner.hpp"

// Redis client includes
#include <boost/asio.hpp>
#include <string>
#include <mutex>
#include <unordered_map>
#include <memory>

// DuckDB secret management includes
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/pair.hpp"

#include <regex>

namespace duckdb {

using boost::asio::ip::tcp;

// Simple Redis protocol formatter
class RedisProtocol {
public:
    static std::string formatAuth(const std::string& password) {
        return "*2\r\n$4\r\nAUTH\r\n$" + std::to_string(password.length()) + "\r\n" + password + "\r\n";
    }

    static std::string parseResponse(const std::string& response) {
        if (response.empty()) return "";
        if (response[0] == '$') {
            // Bulk string response
            size_t pos = response.find("\r\n");
            if (pos == std::string::npos) return "";
            
            // Skip the length prefix and first \r\n
            pos += 2;
            std::string value = response.substr(pos);
            
            // Remove trailing \r\n if present
            if (value.size() >= 2 && value.substr(value.size() - 2) == "\r\n") {
                value = value.substr(0, value.size() - 2);
            }
            return value;
        } else if (response[0] == '+') {
            // Simple string response
            return response.substr(1, response.find("\r\n") - 1);
        } else if (response[0] == '-') {
            // Error response
            throw InvalidInputException("Redis error: " + response.substr(1));
        } else if (response[0] == ':') {
            // Integer response
			return response.substr(1, response.find("\r\n") - 1);
		}
        return response;
    }
    
    static std::string formatZRangeByScore(const std::string& key, const std::string& min, const std::string& max) {
        return std::string("*4\r\n$15\r\nZRANGEBYSCORE\r\n") +
            "$" + std::to_string(key.length()) + "\r\n" + key + "\r\n" +
            "$" + std::to_string(min.length()) + "\r\n" + min + "\r\n" +
            "$" + std::to_string(max.length()) + "\r\n" + max + "\r\n";
    }

	static std::string formatExists(const std::string& key) {
		return "*2\r\n$6\r\nEXISTS\r\n$" + std::to_string(key.length()) + "\r\n" + key + "\r\n";
	}

	static std::string formatZadd(const std::string& key, const std::string& score, const std::string& member) {
		return "*4\r\n$4\r\nZADD\r\n$" +
		    std::to_string(key.length()) + "\r\n" + key + "\r\n" +
		    std::to_string(score.length()) + "\r\n" + score + "\r\n" +
		    std::to_string(member.length()) + "\r\n" + member + "\r\n";
	}

	static std::string formatZrem(const std::string& key, const std::string& member) {
		return "*3\r\n$4\r\nZREM\r\n$" +
		    std::to_string(key.length()) + "\r\n" + key + "\r\n" +
		    std::to_string(member.length()) + "\r\n" + member + "\r\n";
	}

    static std::vector<std::string> parseArrayResponse(const std::string& response) {
        std::vector<std::string> result;
        if (response.empty() || response[0] != '*') return result;
        
        size_t pos = 1;
        size_t end = response.find("\r\n", pos);
        if (end == std::string::npos) return result;
        int array_size;
        try {
            array_size = std::stoi(response.substr(pos, end - pos));
        } catch (const std::invalid_argument& e) {
            return result;
        }
        pos = end + 2;

        for (int i = 0; i < array_size; i++) {
            if (pos >= response.length() || response[pos] != '$') {
                break;
            }
            pos++;
            end = response.find("\r\n", pos);
            if (end == std::string::npos) break;
            
            int str_len;
            try {
                str_len = std::stoi(response.substr(pos, end - pos));
            } catch (const std::invalid_argument& e) {
                break;
            }
            pos = end + 2;
            if (str_len >= 0) {
                if (pos + str_len > response.length()) break;
                result.push_back(response.substr(pos, str_len));
                pos += str_len + 2;
            }
        }
        return result;
    }
};

// Redis connection class
class RedisConnection {
public:
    RedisConnection(const std::string& host, const std::string& port, const std::string& password = "") 
        : io_context_(), socket_(io_context_) {
        try {
            tcp::resolver resolver(io_context_);
            auto endpoints = resolver.resolve(host, port);
            boost::asio::connect(socket_, endpoints);

            if (!password.empty()) {
                std::string auth_cmd = RedisProtocol::formatAuth(password);
                boost::asio::write(socket_, boost::asio::buffer(auth_cmd));
                
                boost::asio::streambuf response;
                boost::asio::read_until(socket_, response, "\r\n");
                
                std::string auth_response((std::istreambuf_iterator<char>(&response)),
                                        std::istreambuf_iterator<char>());
                RedisProtocol::parseResponse(auth_response);
            }
        } catch (std::exception& e) {
            throw InvalidInputException("Redis connection error: " + std::string(e.what()));
        }
    }

    std::string execute(const std::string& command) {
        std::lock_guard<std::mutex> lock(mutex_);
        try {
            boost::asio::write(socket_, boost::asio::buffer(command));
            
            boost::asio::streambuf response;
            boost::asio::read(socket_, response, boost::asio::transfer_at_least(1));
            
            return std::string((std::istreambuf_iterator<char>(&response)),
                             std::istreambuf_iterator<char>());
        } catch (std::exception& e) {
            throw InvalidInputException("Redis execution error: " + std::string(e.what()));
        }
    }

private:
    boost::asio::io_context io_context_;
    tcp::socket socket_;
    std::mutex mutex_;
};

// Connection pool manager
class ConnectionPool {
public:
    static ConnectionPool& getInstance() {
        static ConnectionPool instance;
        return instance;
    }

    std::shared_ptr<RedisConnection> getConnection(const std::string& host, 
                                                  const std::string& port,
                                                  const std::string& password = "") {
        std::string key = host + ":" + port;
        std::lock_guard<std::mutex> lock(mutex_);
        
        auto it = connections_.find(key);
        if (it == connections_.end()) {
            auto conn = std::make_shared<RedisConnection>(host, port, password);
            connections_[key] = conn;
            return conn;
        }
        return it->second;
    }

private:
    ConnectionPool() {}
    std::mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<RedisConnection>> connections_;
};

// Helper to get Redis secret
static bool GetRedisSecret(ClientContext &context, const string &secret_name, string &host, string &port, string &password) {
    auto &secret_manager = SecretManager::Get(context);
    try {
        auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
        auto secret_match = secret_manager.LookupSecret(transaction, secret_name, "redis");
        if (secret_match.HasMatch()) {
            auto &secret = secret_match.GetSecret();
            const auto &kv_secret = dynamic_cast<const KeyValueSecret &>(secret);
            
            Value host_val, port_val, password_val;
			if (!kv_secret.TryGetValue("host", host_val) || !kv_secret.TryGetValue("port", port_val) ||
			    !kv_secret.TryGetValue("password", password_val)) {
				return false;
			}

            host = host_val.ToString();
            port = port_val.ToString();
            password = password_val.ToString();
            return true;
        }
    } catch (...) {
        return false;
    }
    return false;
}

struct GigapiBindData : public TableFunctionData {
	string query;
};

struct GigapiState : public GlobalTableFunctionState {
	unique_ptr<QueryResult> query_result;
};

static unique_ptr<GlobalTableFunctionState> GigapiInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<GigapiState>();
}

static unique_ptr<FunctionData> GigapiBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<GigapiBindData>();
	auto sql_query = input.inputs[0].GetValue<string>();

	Parser parser;
	parser.ParseQuery(sql_query);

	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		throw InvalidInputException("Expected a single SELECT statement for gigapi function");
	}

	auto select_statement_copy = parser.statements[0]->Copy();
	auto &select_statement = select_statement_copy->Cast<SelectStatement>();
	auto &select_node = select_statement.node->Cast<SelectNode>();

	if (!select_node.from_table || select_node.from_table->type != TableReferenceType::BASE_TABLE) {
		throw InvalidInputException("Expected a FROM clause with a single table for gigapi function");
	}
	auto &table_ref = select_node.from_table->Cast<BaseTableRef>();
	string qualified_name =
	    table_ref.schema_name.empty() ? table_ref.table_name : table_ref.schema_name + "." + table_ref.table_name;

	string host, port, password;
	if (!GetRedisSecret(context, "gigapi", host, port, password)) {
		throw InvalidInputException("Gigapi secret not found. Create a redis secret named 'gigapi'.");
	}
	auto redis_conn = ConnectionPool::getInstance().getConnection(host, port, password);
	string redis_key = "giga:idx:ts:" + qualified_name;

	string min_time = "-inf";
	string max_time = "+inf";
	if (select_node.where_clause) {
		vector<DetailedWhereConditionResult> conditions;
		ExtractDetailedWhereConditionsFromExpression(*select_node.where_clause, conditions);

		for (const auto &cond : conditions) {
			if (cond.column_name == "time") {
				try {
					auto timestamp_val = Timestamp::FromString(cond.value);
					auto nanos = timestamp_val.value * 1000;
					string nanos_str = std::to_string(nanos);

					if (cond.operator_type == ">" || cond.operator_type == ">=") {
						min_time = nanos_str;
					} else if (cond.operator_type == "<" || cond.operator_type == "<=") {
						max_time = nanos_str;
					} else if (cond.operator_type == "=") {
						min_time = nanos_str;
						max_time = nanos_str;
					}
				} catch (const std::exception &e) {
				}
			}
		}
	}

	string file_list_response = redis_conn->execute(RedisProtocol::formatZRangeByScore(redis_key, min_time, max_time));
	auto file_list_vec = RedisProtocol::parseArrayResponse(file_list_response);

	if (file_list_vec.empty()) {
		throw InvalidInputException("No files found in Redis for table '%s' in the given time range.", qualified_name);
	}

	vector<Value> file_values;
	for (const auto &file_path : file_list_vec) {
		file_values.emplace_back(file_path);
	}

	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_uniq<ConstantExpression>(Value::LIST(file_values)));

	auto new_table_ref = make_uniq<TableFunctionRef>();
	new_table_ref->function = make_uniq<FunctionExpression>("read_parquet", std::move(children));
	select_node.from_table = std::move(new_table_ref);

	result->query = select_statement_copy->ToString();

	// Execute the rewritten query to get the schema
	Connection db_conn(*context.db);
	auto dummy_result = db_conn.Query(result->query);

	if (dummy_result->HasError()) {
		throw InvalidInputException("Error in rewritten query: %s", dummy_result->GetError());
	}

	for (const auto &column : dummy_result->types) {
		return_types.push_back(column);
	}
	for (const auto &name : dummy_result->names) {
		names.push_back(name);
	}

	return std::move(result);
}

static void GigapiFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<GigapiBindData>();
	auto &state = data_p.global_state->Cast<GigapiState>();

	if (!state.query_result) {
		Connection conn(*context.db);
		state.query_result = conn.Query(bind_data.query);
	}

	auto chunk = state.query_result->Fetch();
	if (!chunk || chunk->size() == 0) {
		output.SetCardinality(0);
		return;
	}
	output.Reference(*chunk);
	output.SetCardinality(chunk->size());
}

ParserExtensionPlanResult gigapi_plan(ParserExtensionInfo *, ClientContext &context,
                                     unique_ptr<ParserExtensionParseData> parse_data) {
	// Throw a binder exception to use our custom binder
	throw BinderException("use gigapi_bind");
}

BoundStatement gigapi_bind(ClientContext &context, Binder &binder, OperatorExtensionInfo *info, SQLStatement &statement) {
	if (statement.type != StatementType::EXTENSION_STATEMENT) {
		return binder.Bind(statement);
	}
	auto &extension_statement = (ExtensionStatement &)statement;
	if (extension_statement.extension.plan_function != gigapi_plan) {
		return binder.Bind(statement);
	}

	// It's our extension, get the real statement from the parse data
	auto &gigapi_parse_data = (GigapiParseData &)*extension_statement.parse_data;
	auto &real_statement = *gigapi_parse_data.statement;

	if (real_statement.type != StatementType::SELECT_STATEMENT) {
		return binder.Bind(real_statement);
	}
	auto &select_statement = (SelectStatement &)real_statement;

	// Safely cast to SelectNode, pass through if it's not a simple SELECT
	auto select_node_ptr = dynamic_cast<SelectNode *>(select_statement.node.get());
	if (!select_node_ptr) {
		return binder.Bind(real_statement);
	}
	auto &select_node = *select_node_ptr;

	// Check if we can handle this query: it must be a SELECT from a single base table
	if (!select_node.from_table || select_node.from_table->type != TableReferenceType::BASE_TABLE) {
		return binder.Bind(real_statement);
	}

	auto &table_ref = dynamic_cast<BaseTableRef &>(*select_node.from_table);
	string qualified_name =
	    table_ref.schema_name.empty() ? table_ref.table_name : table_ref.schema_name + "." + table_ref.table_name;

	// Get Redis connection details from secret, pass through if not found or connection fails
	string host, port, password;
	if (!GetRedisSecret(context, "gigapi", host, port, password)) {
		return binder.Bind(real_statement);
	}
	std::shared_ptr<RedisConnection> redis_conn;
	try {
		redis_conn = ConnectionPool::getInstance().getConnection(host, port, password);
	} catch (const std::exception &e) {
		return binder.Bind(real_statement);
	}

	// Check if a GigAPI index exists for this table
	string redis_key = "giga:idx:ts:" + qualified_name;
	string exists_response = redis_conn->execute(RedisProtocol::formatExists(redis_key));
	if (RedisProtocol::parseResponse(exists_response) != "1") {
		// No index exists, let DuckDB handle it
		return binder.Bind(real_statement);
	}

	// Index exists, proceed with rewrite
	string min_time = "-inf";
	string max_time = "+inf";
	if (select_node.where_clause) {
		vector<DetailedWhereConditionResult> conditions;
		ExtractDetailedWhereConditionsFromExpression(*select_node.where_clause, conditions);

		for (const auto &cond : conditions) {
			if (cond.column_name == "time") {
				try {
					auto timestamp_val = Timestamp::FromString(cond.value);
					auto nanos = timestamp_val.value * 1000;
					string nanos_str = std::to_string(nanos);

					if (cond.operator_type == ">" || cond.operator_type == ">=") {
						min_time = nanos_str;
					} else if (cond.operator_type == "<" || cond.operator_type == "<=") {
						max_time = nanos_str;
					} else if (cond.operator_type == "=") {
						min_time = nanos_str;
						max_time = nanos_str;
					}
				} catch (const std::exception &e) {
					// Ignore conditions with values that can't be parsed as timestamps
				}
			}
		}
	}

	string file_list_response = redis_conn->execute(RedisProtocol::formatZRangeByScore(redis_key, min_time, max_time));
	auto file_list_vec = RedisProtocol::parseArrayResponse(file_list_response);

	if (file_list_vec.empty()) {
		throw InvalidInputException("No files found in Redis for table '%s' in the given time range.",
		                            qualified_name);
	}

	vector<Value> file_values;
	for (const auto &file_path : file_list_vec) {
		file_values.emplace_back(file_path);
	}

	vector<unique_ptr<ParsedExpression>> children;
	children.push_back(make_uniq<ConstantExpression>(Value::LIST(file_values)));

	auto new_table_ref = make_uniq<TableFunctionRef>();
	new_table_ref->function = make_uniq<FunctionExpression>("read_parquet", std::move(children));
	select_node.from_table = std::move(new_table_ref);

	return binder.Bind(real_statement);
}

ParserExtensionParseResult gigapi_parse(ParserExtensionInfo *, const std::string &query) {
	Parser parser;
	try {
		parser.ParseQuery(query);
	} catch (const std::exception &e) {
		// Let the default parser handle the error.
		return ParserExtensionParseResult();
	}
	return ParserExtensionParseResult(
	    make_uniq_base<ParserExtensionParseData, GigapiParseData>(std::move(parser.statements[0])));
}

struct GigapiParserExtension : public ParserExtension {
	GigapiParserExtension() {
		parse_function = gigapi_parse;
		plan_function = gigapi_plan;
	}
};

static void GigapiTestCreateEmptyIndexFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &table_name_vector = args.data[0];
	auto &context = state.GetContext();

	UnaryExecutor::Execute<string_t, bool>(
	    table_name_vector, result, args.size(), [&](string_t table_name) {
		    string host, port, password;
		    if (!GetRedisSecret(context, "gigapi", host, port, password)) {
			    return false;
		    }
		    auto redis_conn = ConnectionPool::getInstance().getConnection(host, port, password);
		    string redis_key = "giga:idx:ts:" + table_name.GetString();
		    string dummy_member = "placeholder";

		    // Add and immediately remove a member to ensure the key exists as an empty sorted set
		    redis_conn->execute(RedisProtocol::formatZadd(redis_key, "0", dummy_member));
		    redis_conn->execute(RedisProtocol::formatZrem(redis_key, dummy_member));

		    return true;
	    });
}

static void GigapiDryRunFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &sql_query_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(
	    sql_query_vector, result, args.size(), [&](string_t sql_query) {
		    // Parse the query
		    Parser parser;
		    parser.ParseQuery(sql_query.GetString());

		    if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
			    throw InvalidInputException("gigapi_dry_run expects a single SELECT statement");
		    }

		    auto select_statement = parser.statements[0]->Copy(); // Make a copy to modify
		    auto &select_stmt_ref = select_statement->Cast<SelectStatement>();
		    auto &select_node = select_stmt_ref.node->Cast<SelectNode>();

		    if (!select_node.from_table || select_node.from_table->type != TableReferenceType::BASE_TABLE) {
			    throw InvalidInputException("gigapi_dry_run expects a SELECT from a single table");
		    }

		    // For a dry run, we use a dummy file list.
			vector<Value> dummy_files;
			dummy_files.emplace_back("dummy/file1.parquet");
			dummy_files.emplace_back("dummy/file2.parquet");

		    // Create a new table reference for read_parquet
			vector<unique_ptr<ParsedExpression>> children;
			children.push_back(make_uniq<ConstantExpression>(Value::LIST(dummy_files)));

			auto new_table_ref = make_uniq<TableFunctionRef>();
			new_table_ref->function = make_uniq<FunctionExpression>("read_parquet", std::move(children));

		    // Replace the FROM clause of the original query
		    select_node.from_table = std::move(new_table_ref);

		    string rewritten_query = select_statement->ToString();
		    return StringVector::AddString(result, rewritten_query);
	});
}

static void LoadInternal(DatabaseInstance &instance) {
	CreateRedisSecretFunctions::Register(instance);
	
	auto &config = DBConfig::GetConfig(instance);
	config.parser_extensions.push_back(GigapiParserExtension());
	config.operator_extensions.push_back(make_uniq<GigapiOperatorExtension>());

	// Add the table function for testing
	TableFunction gigapi_func("gigapi", {LogicalType::VARCHAR}, GigapiFunction, GigapiBind, GigapiInit);
	ExtensionUtil::RegisterFunction(instance, gigapi_func);

	// Add the dry run function for testing
	auto gigapi_dry_run_scalar = ScalarFunction("gigapi_dry_run", {LogicalType::VARCHAR}, LogicalType::VARCHAR, GigapiDryRunFunction);
	ExtensionUtil::RegisterFunction(instance, gigapi_dry_run_scalar);

	// Add a test-only function to create empty indexes in Redis
	auto giga_test_create_empty_index_scalar = ScalarFunction("giga_test_create_empty_index", {LogicalType::VARCHAR}, LogicalType::BOOLEAN, GigapiTestCreateEmptyIndexFunction);
	ExtensionUtil::RegisterFunction(instance, giga_test_create_empty_index_scalar);
}

void GigapiExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string GigapiExtension::Name() {
	return "gigapi";
}

std::string GigapiExtension::Version() const {
#ifdef EXT_VERSION_GIGAPI
	return EXT_VERSION_GIGAPI;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void gigapi_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::GigapiExtension>();
}

DUCKDB_EXTENSION_API const char *gigapi_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
