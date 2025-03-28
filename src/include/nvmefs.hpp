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

struct NvmeDeviceGeometry {
	uint64_t lba_size;
	uint64_t lba_count;
};

typedef void *nvme_buf_ptr;

class NvmeFileHandle;
typedef NvmeFileHandle MetadataFileHandle;
class NvmeFileHandle : public FileHandle {
	friend class NvmeFileSystem;

public:
	NvmeFileHandle(FileSystem &file_system, string path, uint8_t plid_idx, xnvme_dev *device, uint8_t plid_count,
	               FileOpenFlags flags, bool internal_fileHandle = false);
	~NvmeFileHandle() override;

	void Read(void *buffer, idx_t nr_bytes, idx_t location);
	void Write(void *buffer, idx_t nr_bytes, idx_t location);
	int64_t Read(void *buffer, idx_t nr_bytes);
	int64_t Write(void *buffer, idx_t nr_bytes);

	idx_t GetFileSize();
	void Sync();

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

	/// @brief Set the cursor of the current file handle. This new position will work as an offset being applied for
	/// future read and write operations
	/// @param location The location to set the file pointer to
	void SetFilePointer(idx_t location);

	/// @brief Gets the current location(in bytes) of the file pointer
	/// @return The current location of the file pointer in bytes
	idx_t GetFilePointer();

	void Close() {
	}

protected:
	xnvme_dev *device;
	uint32_t placement_identifier;
	uint8_t placement_identifier_count;

private:
	bool internal_fileHandle; // Means that this file handle is used in the context of another file handle
	uint64_t cursor_offset;
};

class NvmeFileSystemProxy;
class NvmeFileSystem : public FileSystem {
	friend class NvmeFileSystemProxy;

public:
	NvmeFileSystem(NvmeFileSystemProxy &proxy_ref);
	NvmeFileSystem(NvmeFileSystemProxy &proxy_ref, const string &device_path, const uint64_t plhdls);
	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener = nullptr) override;

	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	bool CanHandleFile(const string &fpath) override;
	void Seek(FileHandle &handle, idx_t location) override;
	idx_t SeekPosition(FileHandle &handle) override;

	string GetName() const {
		return "NvmeFileSystem";
	}

protected:
	unique_ptr<NvmeDeviceGeometry> GetDeviceGeometry();
	uint8_t GetPlacementIdentifierIndexOrDefault(const string &path);
	uint64_t WriteInternal(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location_lba,
	                       idx_t in_block_offset);
	uint64_t ReadInternal(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location_lba,
	                      idx_t in_block_offset);

	/// @brief Opens a file handle for metadata in the context of a given file handle
	/// @param handle The file handle to get context from
	/// @param path The "internal" metadata file path
	/// @return FileHandle specifically for the metadata section of the device
	unique_ptr<MetadataFileHandle> OpenMetadataFile(FileHandle &handle, string path, FileOpenFlags flags);

private:
	map<string, uint8_t> allocated_placement_identifiers;
	vector<string> allocated_paths;
	NvmeFileSystemProxy &proxy_filesystem;
	string device_path;
	uint64_t plhdls;
};

} // namespace duckdb
