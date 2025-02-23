#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/map.hpp"
#include "nvmefs_proxy.hpp"

#include <libxnvme.h>

#define NVMEFS_PATH_PREFIX "nvmefs://"
#define FDP_PLID_COUNT     8

namespace duckdb {

struct NvmeCmdContext {
	xnvme_cmd_ctx ctx;
	uint32_t namespace_id;
	uint64_t lba_size;
};

class NvmeFileHandle : public FileHandle {
	friend class NvmeFileSystem;

public:
	NvmeFileHandle(FileSystem &file_system, string path, uint8_t plid_idx, xnvme_dev *device);
	~NvmeFileHandle() override;

	void Read(void *buffer, idx_t nr_bytes, idx_t location);
	void Write(void *buffer, idx_t nr_bytes, idx_t location);
	int64_t Read(void *buffer, idx_t nr_bytes);
	int64_t Write(void *buffer, idx_t nr_bytes);

	unique_ptr<NvmeCmdContext> PrepareWriteCommand();
	unique_ptr<NvmeCmdContext> PrepareReadCommand();

	void Close() {
	}

protected:
	xnvme_dev *device;
	uint32_t placement_identifier;
};

class NvmeFileSystemProxy;
class NvmeFileSystem : public FileSystem {
public:
	NvmeFileSystem(NvmeFileSystemProxy &proxy_ref);
	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener = nullptr) override;
	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	bool CanHandleFile(const string &fpath) override;

	string GetName() const {
		return "NvmeFileSystem";
	}

protected:
	uint8_t GetPlacementIdentifierIndexOrDefault(const string &path);

private:
	map<string, uint8_t> allocated_placement_identifiers;
	vector<string> allocated_paths;
	NvmeFileSystemProxy &proxy_filesystem;
};

} // namespace duckdb
