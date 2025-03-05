#pragma once

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/map.hpp"
#include "nvmefs_proxy.hpp"

#include <libxnvme.h>

#define NVMEFS_PATH_PREFIX "nvmefs://"

namespace duckdb {

struct NvmeCmdContext {
	xnvme_cmd_ctx ctx;
	uint32_t namespace_id;
	uint64_t number_of_lbas;
};

typedef void *nvme_buf_ptr;

class NvmeFileHandle : public FileHandle {
	friend class NvmeFileSystem;

public:
	NvmeFileHandle(FileSystem &file_system, string path, uint8_t plid_idx, xnvme_dev *device, uint8_t plid_count,
	               FileOpenFlags flags);
	~NvmeFileHandle() override;

	void Read(void *buffer, idx_t nr_bytes, idx_t location);
	void Write(void *buffer, idx_t nr_bytes, idx_t location);
	int64_t Read(void *buffer, idx_t nr_bytes);
	int64_t Write(void *buffer, idx_t nr_bytes);

protected:
	unique_ptr<NvmeCmdContext> PrepareWriteCommand(int64_t nr_bytes);
	unique_ptr<NvmeCmdContext> PrepareReadCommand(int64_t nr_bytes);

	/// @brief Allocates a device specific buffer. After the need for the created buffer is gone, it should be freed
	/// using FreeDeviceBuffer
	/// @param nr_bytes The number of bytes to allocate (The actual allocation might be larger)
	/// @return A pointer to the allocated buffer
	nvme_buf_ptr AllocateDeviceBuffer(int64_t nr_bytes);

	/// @brief Frees a device specific buffer
	/// @param buffer The buffer to free
	void FreeDeviceBuffer(nvme_buf_ptr buffer);

	void Close() {
	}

protected:
	xnvme_dev *device;
	uint32_t placement_identifier;
};

class NvmeFileSystemProxy;
class NvmeFileSystem : public FileSystem {
	friend class NvmeFileSystemProxy;
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
	uint64_t WriteInternal(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location);

private:
	map<string, uint8_t> allocated_placement_identifiers;
	vector<string> allocated_paths;
	NvmeFileSystemProxy &proxy_filesystem;
};

} // namespace duckdb
