#pragma once

#include "duckdb.h"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/map.hpp"
#include "nvmefs.hpp"

#include <string>

namespace duckdb {

#define NVMEFS_METADATA_LOCATION 0
// TODO: Use NVME_PREFIX_PATH instead of hardcode 'nvmefs://'
const std::string NVME_GLOBAL_METADATA_PATH = "nvmefs://.global_metadata";

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
	Metadata database;
	Metadata write_ahead_log;
	Metadata temporary;
};

class NvmeFileSystem;
class NvmeFileSystemProxy : public FileSystem {
public:
	NvmeFileSystemProxy();
	~NvmeFileSystemProxy() = default;

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener = nullptr) override;
	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	bool CanHandleFile(const string &fpath) override;

	string GetName() const {
		return "NvmeFileSystemProxy";
	}

	GlobalMetadata GetMetadata() {
		return *metadata;
	}

private:
	unique_ptr<GlobalMetadata> LoadMetadata(optional_ptr<FileOpener> opener);
	unique_ptr<GlobalMetadata> InitializeMetadata(optional_ptr<FileOpener> opener);
	void WriteMetadata(uint64_t location, uint64_t nr_lbas, MetadataType type);
	MetadataType GetMetadataType(string path);
	uint64_t GetLBA(MetadataType type, string filename, idx_t location);

private:
	Allocator &allocator;
	unique_ptr<GlobalMetadata> metadata;
	unique_ptr<NvmeFileSystem> fs;
	map<std::string, uint64_t> file_to_lba;
};

} // namespace duckdb
