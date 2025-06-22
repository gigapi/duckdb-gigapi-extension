#define DUCKDB_EXTENSION_MAIN

#include "gigapi_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/extension_statement.hpp"
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

GigapiParseData::GigapiParseData(unique_ptr<SQLStatement> statement_p) : statement(std::move(statement_p)) {
}

unique_ptr<ParserExtensionParseData> GigapiParseData::Copy() const {
	return make_uniq<GigapiParseData>(statement->Copy());
}

string GigapiParseData::ToString() const {
	return statement->ToString();
}

GigapiState::GigapiState(unique_ptr<ParserExtensionParseData> parse_data_p) : parse_data(std::move(parse_data_p)) {
}

struct GigapiBindData : public TableFunctionData {
	string query;
};

struct GigapiData : public GlobalTableFunctionState {
	unique_ptr<QueryResult> query_result;
};

static unique_ptr<GlobalTableFunctionState> GigapiInit(ClientContext &context, TableFunctionInitInput &input) {
	auto bind_data = input.bind_data->Copy();
	auto &gigapi_bind_data = dynamic_cast<GigapiBindData &>(*bind_data);

	Connection con(*context.db);
	auto query_result = con.Query(gigapi_bind_data.query);

	auto result = make_uniq<GigapiData>();
	result->query_result = std::move(query_result);
	return std::move(result);
}

static unique_ptr<FunctionData> GigapiBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {

	auto result = make_uniq<GigapiBindData>();
	result->query = input.inputs[0].GetValue<string>();

	Connection con(*context.db);
	auto statement = con.ExtractStatements(result->query);
	if (statement.empty()) {
		throw InvalidInputException("No statements found in query");
	}

	auto stream_result = con.SendQuery(result->query);
	while (true) {
		auto chunk = stream_result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		for (idx_t i = 0; i < chunk->ColumnCount(); i++) {
			return_types.push_back(chunk->GetTypes()[i]);
			names.push_back(stream_result->names[i]);
		}
		break;
	}

	return std::move(result);
}

static void GigapiFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = dynamic_cast<GigapiData &>(*data_p.global_state);
	if (!data.query_result) {
		return;
	}

	auto chunk = data.query_result->Fetch();
	if (!chunk || chunk->size() == 0) {
		return;
	}

	output.Move(*chunk);
	output.SetCardinality(chunk->size());
}

BoundStatement gigapi_bind(ClientContext &context, Binder &binder, OperatorExtensionInfo *info, SQLStatement &statement) {
	if (statement.type != StatementType::EXTENSION_STATEMENT) {
		return {};
	}
	auto &extension_statement = dynamic_cast<ExtensionStatement &>(statement);
	if (extension_statement.extension.parse_function != gigapi_parse) {
		return {};
	}

	auto lookup = context.registered_state->Get<GigapiState>("gigapi_state");
	if (!lookup) {
		return {};
	}
	auto gigapi_state = (GigapiState *)lookup.get();
	auto gigapi_binder = Binder::CreateBinder(context, &binder);
	auto gigapi_parse_data = dynamic_cast<GigapiParseData *>(gigapi_state->parse_data.get());
	if (!gigapi_parse_data) {
		return {};
	}

	// At this point, you can inspect the query and decide what to do.
	// For now, we're just binding the original statement.
	// You could modify the statement or create a new one.
	return gigapi_binder->Bind(*(gigapi_parse_data->statement));
}

ParserExtensionPlanResult gigapi_plan(ParserExtensionInfo *, ClientContext &context,
                                      unique_ptr<ParserExtensionParseData> parse_data) {
	// We stash away the ParserExtensionParseData before throwing an exception
	// here. This allows the planning to be picked up by gigapi_bind instead, but
	// we're not losing important context.
	auto gigapi_state = make_shared_ptr<GigapiState>(std::move(parse_data));
	context.registered_state->Remove("gigapi_state");
	context.registered_state->Insert("gigapi_state", gigapi_state);
	throw BinderException("Use gigapi_bind instead");
}

ParserExtensionParseResult gigapi_parse(ParserExtensionInfo *, const std::string &query) {
	Parser parser;
	try {
		parser.ParseQuery(query);
	} catch (...) {
		// Let DuckDB handle parser errors
		return ParserExtensionParseResult();
	}

	if (parser.statements.size() != 1) {
		// We only support single statements for now
		return ParserExtensionParseResult();
	}

	return ParserExtensionParseResult(make_uniq_base<ParserExtensionParseData, GigapiParseData>(std::move(parser.statements[0])));
}

GigapiParserExtension::GigapiParserExtension() {
	parse_function = gigapi_parse;
	plan_function = gigapi_plan;
}

GigapiOperatorExtension::GigapiOperatorExtension() {
	Bind = gigapi_bind;
}

std::string GigapiOperatorExtension::GetName() {
	return "gigapi";
}

unique_ptr<LogicalExtensionOperator> GigapiOperatorExtension::Deserialize(Deserializer &deserializer) {
	throw NotImplementedException("GigapiOperatorExtension cannot be deserialized");
}

static void GigapiTestCreateEmptyIndexFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &table_name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(
	    table_name_vector, result, args.size(), [&](string_t table_name) {
		    auto &context = state.GetContext();
		    string host, port, password;
		    if (!GetRedisSecret(context, "gigapi", host, port, password)) {
			    throw InvalidInputException("Gigapi secret not found. Create a redis secret named 'gigapi'.");
		    }
		    auto redis_conn = ConnectionPool::getInstance().getConnection(host, port, password);

		    string redis_key = "giga:idx:ts:" + table_name.GetString();
		    string response = redis_conn->execute(RedisProtocol::formatZadd(redis_key, "0", "dummy"));

		    return StringVector::AddString(result, "Index created for " + table_name.GetString());
	    });
}

static void GigapiDryRunFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &sql_query_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(
	    sql_query_vector, result, args.size(), [&](string_t sql_query) {
		    auto &context = state.GetContext();
		    auto sql_query_str = sql_query.GetString();

		    Parser parser;
		    parser.ParseQuery(sql_query_str);
		    if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
			    throw InvalidInputException("Gigapi can only rewrite simple SELECT queries.");
		    }
		    auto select_statement = unique_ptr_cast<SQLStatement, SelectStatement>(std::move(parser.statements[0]));

		    Planner planner(context);
		    planner.CreatePlan(std::move(select_statement));
		    auto plan = std::move(planner.plan);

		    return StringVector::AddString(result, plan->ToString());
	    });
}


static void LoadInternal(DatabaseInstance &instance) {
	// Register the scalar function
	auto &catalog = Catalog::GetSystemCatalog(instance);

	TableFunction gigapi_func("gigapi", {LogicalType::VARCHAR}, GigapiFunction, GigapiBind, GigapiInit);
	ExtensionUtil::RegisterFunction(instance, gigapi_func);

	// gigapi_test_create_empty_index
	ScalarFunction gigapi_test_create_empty_index_func("gigapi_test_create_empty_index", {LogicalType::VARCHAR},
	                                                   LogicalType::VARCHAR, GigapiTestCreateEmptyIndexFunction);
	gigapi_test_create_empty_index_func.null_handling = FunctionNullHandling::SPECIAL_HANDLING;

	ExtensionUtil::RegisterFunction(instance, gigapi_test_create_empty_index_func);

	// gigapi_dry_run
	ScalarFunction gigapi_dry_run_func("gigapi_dry_run", {LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                                   GigapiDryRunFunction);
	gigapi_dry_run_func.null_handling = FunctionNullHandling::SPECIAL_HANDLING;

	ExtensionUtil::RegisterFunction(instance, gigapi_dry_run_func);

	auto &config = DBConfig::GetConfig(instance);
	config.parser_extensions.push_back(GigapiParserExtension());
	config.operator_extensions.push_back(make_uniq<GigapiOperatorExtension>());
}

void GigapiExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}

std::string GigapiExtension::Name() {
	return "gigapi";
}

std::string GigapiExtension::Version() const {
#ifdef EXTENSION_VERSION_GIGAPI
	return EXTENSION_VERSION_GIGAPI;
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
