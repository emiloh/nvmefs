#include "nvmefs_config.hpp"

#include "duckdb/main/extension_util.hpp"

namespace duckdb {

const unordered_set<string> NVMEFS_BACKENDS_ASYNC = {
    "io_uring", "io_uring_cmd", "spdk_async", "libaio", "io_ring", "iocp", "iocp_th", "posix", "emu", "thrpool", "nil"};

const unordered_set<string> NVMEFS_BACKENDS_SYNC = {"spdk_sync", "nvme"};

static unique_ptr<BaseSecret> CreateNvmefsSecretFromConfig(ClientContext &context, CreateSecretInput &input) {
	auto scope = input.scope;

	if (scope.empty()) {
		scope.push_back("nvmefs://");
	}

	auto config = make_uniq<KeyValueSecret>(scope, input.type, input.provider, input.name);

	for (const auto &pair : input.options) {
		auto lower = StringUtil::Lower(pair.first);
		config->secret_map[lower] = pair.second;
	}

	return std::move(config);
}

void SetNvmefsSecretParameters(CreateSecretFunction &function) {
	function.named_parameters["nvme_device_path"] = LogicalType::VARCHAR;
	function.named_parameters["fdp_plhdls"] = LogicalType::BIGINT;
	function.named_parameters["backend"] = LogicalType::VARCHAR;
}

void RegisterCreateNvmefsSecretFunciton(DatabaseInstance &instance) {
	string type = "nvmefs";

	SecretType secret_type;
	secret_type.name = type;
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";

	ExtensionUtil::RegisterSecretType(instance, secret_type);

	CreateSecretFunction config_function = {type, "config", CreateNvmefsSecretFromConfig};
	SetNvmefsSecretParameters(config_function);
	ExtensionUtil::RegisterFunction(instance, config_function);
}

void CreateNvmefsSecretFunctions::Register(DatabaseInstance &instance) {
	RegisterCreateNvmefsSecretFunciton(instance);
}

NvmeConfig NvmeConfigManager::LoadConfig(DatabaseInstance &instance) {
	DBConfig &config = DBConfig::GetConfig(instance);

	// Change global settings
	TempDirectorySetting::SetGlobal(&instance, config, Value("nvmefs:///tmp"));

	KeyValueSecretReader secret_reader(instance, "nvmefs", "nvmefs://");

	string device;
	string backend;
	int64_t plhdls = 0;
	// TODO: ensure that we always have value here. It is possible to not have value
	idx_t max_temp_size = 76ULL << 30; // ~76 GiB
	if (config.options.maximum_swap_space != DConstants::INVALID_INDEX) {
		max_temp_size = static_cast<idx_t>(config.options.maximum_swap_space);
	}
	idx_t max_wal_size = 1ULL << 25; // 32 MiB

	secret_reader.TryGetSecretKeyOrSetting<string>("nvme_device_path", "nvme_device_path", device);
	secret_reader.TryGetSecretKeyOrSetting<string>("backend", "backend", backend);
	secret_reader.TryGetSecretKeyOrSetting<int64_t>("fdp_plhdls", "fdp_plhdls", plhdls);

	config.AddExtensionOption("nvme_device_path", "Path to NVMe device", {LogicalType::VARCHAR}, Value(device));
	config.AddExtensionOption("fdp_plhdls", "Amount of available placement handlers on the device",
	                          {LogicalType::BIGINT}, Value(plhdls));
	config.AddExtensionOption("backend", "xnvme backend used for IO", {LogicalType::VARCHAR}, Value(backend));

	backend = SanatizeBackend(backend);

	return NvmeConfig {.device_path = device,
	                   .backend = backend,
	                   .async = IsAsynchronousBackend(backend),
	                   .plhdls = static_cast<idx_t>(plhdls),
	                   .max_temp_size = max_temp_size,
	                   .max_wal_size = max_wal_size};
}

bool NvmeConfigManager::IsAsynchronousBackend(const string &backend) {
	return NVMEFS_BACKENDS_ASYNC.find(backend) != NVMEFS_BACKENDS_ASYNC.end();
}

string NvmeConfigManager::SanatizeBackend(const string &backend) {

	if (backend.empty() || (NVMEFS_BACKENDS_SYNC.find(backend) == NVMEFS_BACKENDS_SYNC.end() &&
	                        NVMEFS_BACKENDS_ASYNC.find(backend) == NVMEFS_BACKENDS_ASYNC.end())) {
		return "nvme";
	}

	if (StringUtil::Equals(backend.data(), "spdk_async") || StringUtil::Equals(backend.data(), "spdk_sync")) {
		return "spdk";
	}
	return backend;
}

} // namespace duckdb
