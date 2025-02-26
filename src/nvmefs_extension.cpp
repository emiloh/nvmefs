#define DUCKDB_EXTENSION_MAIN

#include "nvmefs_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "nvmefs_proxy.hpp"
#include "nvmefs_secret.hpp"

namespace duckdb {
struct ConfigPrintFunctionData : public TableFunctionData {
	ConfigPrintFunctionData() {
	}

	bool finished = false;
};

static void ConfigPrint(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<ConfigPrintFunctionData>();

	if (data.finished) {
		return;
	}

	vector<string> settings {"nvme_device_path", "fdp_plhdls", "temp_directory"};
	idx_t chunk_count = 0;

	for (string setting : settings) {
		Value current_value;
		context.TryGetCurrentSetting(setting, current_value);
		output.SetValue(0, chunk_count, Value(setting));
		output.SetValue(1, chunk_count, current_value);
		chunk_count++;
	}

	output.SetCardinality(chunk_count);

	data.finished = true;
}

static unique_ptr<FunctionData> ConfigPrintBind(ClientContext &ctx, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("Setting");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("Value");
	return_types.emplace_back(LogicalType::VARCHAR);

	auto result = make_uniq<ConfigPrintFunctionData>();
	result->finished = false;

	return std::move(result);
}

static void AddConfig(DatabaseInstance &instance) {
	DBConfig &config = DBConfig::GetConfig(instance);

	auto &fs = instance.GetFileSystem();
	KeyValueSecretReader secret_reader(instance, "nvmefs", "nvmefs://");

	string device;
	int plhdls = 0;

	secret_reader.TryGetSecretKeyOrSetting<string>("nvme_device_path", "nvme_device_path", device);
	secret_reader.TryGetSecretKeyOrSetting<int>("fdp_plhdls", "fdp_plhdls", plhdls);

	config.AddExtensionOption("nvme_device_path", "Path to NVMe device", {LogicalType::VARCHAR}, Value(device));
	config.AddExtensionOption("fdp_plhdls", "Amount of available placement handlers on the device",
	                          {LogicalType::BIGINT}, Value(plhdls));
}

static void LoadInternal(DatabaseInstance &instance) {
	// Register NvmeFileSystem
	auto &fs = instance.GetFileSystem();

	fs.RegisterSubSystem(make_uniq<NvmeFileSystemProxy>());

	CreateNvmefsSecretFunctions::Register(instance);
	AddConfig(instance);

	TableFunction config_print_function("print_config", {}, ConfigPrint, ConfigPrintBind);
	ExtensionUtil::RegisterFunction(instance, config_print_function);
}

void NvmefsExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string NvmefsExtension::Name() {
	return "nvmefs";
}

std::string NvmefsExtension::Version() const {
#ifdef EXT_VERSION_NVMEFS
	return EXT_VERSION_NVMEFS;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void nvmefs_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::NvmefsExtension>();
}

DUCKDB_EXTENSION_API const char *nvmefs_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
