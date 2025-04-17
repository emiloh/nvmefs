#pragma once

#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/map.hpp"

#include "device.hpp"
#include "nvme_device.hpp"
#include "nvmefs_config.hpp"

namespace duckdb {

constexpr idx_t NVMEFS_GLOBAL_METADATA_LOCATION = 0;
constexpr char NVMEFS_MAGIC_BYTES[] = "NVMEFS";
const string NVMEFS_PATH_PREFIX = "nvmefs://";
const string NVMEFS_GLOBAL_METADATA_PATH = "nvmefs://.global_metadata";

enum MetadataType { DATABASE, WAL, TEMPORARY };

struct Metadata {
	uint64_t start;
	uint64_t end;
	uint64_t location;
};

struct GlobalMetadata {
	uint64_t db_path_size;
	char db_path[101];

	Metadata database;
	Metadata write_ahead_log;
	Metadata temporary;
};

struct TemporaryFileMetadata {
	uint64_t start;
	uint64_t end;
};

class NvmeFileHandle : public FileHandle {

	friend class NvmeFileSystem;

public:
	NvmeFileHandle(FileSystem &file_system, string path, FileOpenFlags flags);
	~NvmeFileHandle() = default;

	void Read(void *buffer, idx_t nr_bytes, idx_t location);
	void Write(void *buffer, idx_t nr_bytes, idx_t location);

	idx_t GetFileSize();
	void Sync();

	void Close() override;

private:
	unique_ptr<CmdContext> PrepareWriteCommand(idx_t nr_bytes, idx_t start_lba, idx_t offset);
	unique_ptr<CmdContext> PrepareReadCommand(idx_t nr_bytes, idx_t start_lba, idx_t offset);

	/// @brief Calculates the amount of LBAs required to store the given number of bytes
	/// @param nr_bytes The number of bytes to store
	/// @return The number of LBAs required to store the given number of bytes
	idx_t CalculateRequiredLBACount(idx_t nr_bytes);

	void SetFilePointer(idx_t location);
	idx_t GetFilePointer();

private:
	idx_t cursor_offset;
};

class NvmeFileSystem : public FileSystem {
public:
	NvmeFileSystem(NvmeConfig config);
	NvmeFileSystem(NvmeConfig config, unique_ptr<Device> device);
	~NvmeFileSystem() = default;

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener = nullptr) override;
	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes);
	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes);
	bool CanHandleFile(const string &fpath) override;
	bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	int64_t GetFileSize(FileHandle &handle) override;
	void FileSync(FileHandle &handle) override;
	bool OnDiskFile(FileHandle &handle) override;
	void Truncate(FileHandle &handle, int64_t new_size) override;
	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	void RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	void CreateDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override;
	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	void Seek(FileHandle &handle, idx_t location) override;
	idx_t SeekPosition(FileHandle &handle) override;

	Device &GetDevice();

	string GetName() const {
		return "NvmeFileSystem";
	}

private:
	bool TryLoadMetadata();
	void InitializeMetadata(const string &filename);
	unique_ptr<GlobalMetadata> ReadMetadata();
	void WriteMetadata(GlobalMetadata &global);
	void UpdateMetadata(CmdContext &Context);
	MetadataType GetMetadataType(const string &filename);
	idx_t GetLBA(const string &filename, idx_t location);
	idx_t GetStartLBA(const string &filename);
	idx_t GetLocationLBA(const string &filename);
	idx_t GetEndLBA(const string &filename);

	/// @brief Checks that the start_lba is within the assigned metadata range and that lba_start+lba_count is within
	/// the assigned metadata range
	/// @param filename name of the file to check
	/// @param start_lba Start LBA of the IO operation to be performed
	/// @param lba_count Number of LBAs to be read/written
	/// @return True if it is in range, false otherwise
	bool IsLBAInRange(const string &filename, idx_t start_lba, idx_t lba_count);

private:
	Allocator &allocator;
	unique_ptr<GlobalMetadata> metadata;
	unique_ptr<Device> device;
	map<string, TemporaryFileMetadata> file_to_temp_meta;
	idx_t max_temp_size;
	idx_t max_wal_size;
};
} // namespace duckdb
