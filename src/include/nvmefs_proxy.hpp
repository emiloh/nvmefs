#pragma once

#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/storage/storage_info.hpp"

#include "nvmefs.hpp"

#include <string>

namespace duckdb {

#define NVMEFS_METADATA_LOCATION 0
// TODO: Use NVME_PREFIX_PATH instead of hardcode 'nvmefs://'
const std::string NVME_GLOBAL_METADATA_PATH = "nvmefs://.global_metadata";
// TODO: Do not use magic constants here. Possibly get both from configuration.
constexpr uint64_t LBAS_PER_LOCATION = DUCKDB_BLOCK_ALLOC_SIZE / 4096;

enum MetadataType { DATABASE, WAL, TEMPORARY };

struct Metadata {
	uint64_t start;
	uint64_t end;
	uint64_t location;

	// db_end
	// start end -> tmp wal
	// tmp_head wal_head
	// mapping fil -> (start, størrelse)
};

struct GlobalMetadata {
	uint64_t db_path_size;
	// TODO: use string instead
	char db_path[101];

	Metadata database;
	Metadata write_ahead_log;
	Metadata temporary;
};

class NvmeFileSystem;
typedef NvmeFileHandle MetadataFileHandle;
class NvmeFileSystemProxy : public FileSystem {
public:
	NvmeFileSystemProxy();
	~NvmeFileSystemProxy() = default;

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener = nullptr) override;
	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes);
	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes);
	bool CanHandleFile(const string &fpath) override;
	bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;

	string GetName() const {
		return "NvmeFileSystemProxy";
	}

private:
	bool TryLoadMetadata(optional_ptr<FileOpener> opener);
	void InitializeMetadata(FileHandle &handle, string path);
	unique_ptr<GlobalMetadata> ReadMetadata(FileHandle &handle);
	void WriteMetadata(MetadataFileHandle &handle, GlobalMetadata *global);
	void UpdateMetadata(FileHandle &handle, uint64_t location, uint64_t nr_lbas, MetadataType type);
	MetadataType GetMetadataType(string path);
	uint64_t GetLBA(MetadataType type, string filename, idx_t location);
	uint64_t GetStartLBA(MetadataType type, string filename);

private:
	Allocator &allocator;
	unique_ptr<GlobalMetadata> metadata;
	unique_ptr<NvmeFileSystem> fs;
	map<std::string, uint64_t> file_to_lba;
};

} // namespace duckdb
