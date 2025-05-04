#pragma once

#include "duckdb.hpp"
#include "nvmefs_temporary_block_manager.hpp"
#include <atomic>

namespace duckdb {

class TempFileMetadata {
public:
	TempFileMetadata() : file_index(0), block_size(0), nr_blocks(0), block_range(nullptr) {
	}

	std::atomic<bool> is_active;
	idx_t file_index;
	idx_t block_size;
	idx_t nr_blocks;
	std::atomic<idx_t> lba_location;
	TemporaryBlock *block_range;
};

class TemporaryFileMetadataManager {
public:
	TemporaryFileMetadataManager(idx_t start_lba, idx_t end_lba, idx_t lba_size)
	    : block_manager(make_uniq<NvmeTemporaryBlockManager>(start_lba, end_lba)), lba_size(lba_size),
	      lba_amount(end_lba - start_lba) {
	}

	void CreateFile(const string &filename);

	idx_t GetLBA(const string &filename, idx_t lba_location);

	void TruncateFile(const string &filename, idx_t new_size);

	void DeleteFile(const string &filename);

	void MoveLBALocation(const string &filename, idx_t lba_location);

	bool FileExists(const string &filename);

	idx_t GetFileSize(const string &filename);

	void ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback);

	idx_t GetAvailableSpace();

	void Clear();

private:
	idx_t lba_size;
	idx_t lba_amount;
	unique_ptr<NvmeTemporaryBlockManager> block_manager;
	map<string, unique_ptr<TempFileMetadata>> file_to_temp_meta;
	std::mutex alloc_lock;
};
} // namespace duckdb
