#include "temporary_file_metadata_manager.hpp"

namespace duckdb {

inline unique_ptr<TempFileMetadata> CreateTempFileMetadata(const string &filename) {

	unique_ptr<TempFileMetadata> tfmeta = make_uniq<TempFileMetadata>();
	tfmeta->is_active.store(true);

	// Find the position of the first number
	size_t first_number_start = filename.find_last_of('_') + 1;       // Start after the last '_'
	size_t first_number_end = filename.find('-', first_number_start); // Find the '-' after the first number

	// Extract the first number
	std::string block_size_str = filename.substr(first_number_start, first_number_end - first_number_start);
	idx_t block_size = std::stoi(block_size_str);

	// Find the position of the second number
	size_t file_index_start = first_number_end + 1;               // Start after the '-'
	size_t file_index_end = filename.find('.', file_index_start); // Find the '.' after the second number

	// Extract the second number
	std::string file_index_str = filename.substr(file_index_start, file_index_end - file_index_start);
	int file_index = std::stoi(file_index_str);

	tfmeta->block_size = block_size;
	tfmeta->file_index = file_index;
	tfmeta->nr_blocks = (1 << file_index) * 4000;
	tfmeta->lba_location.store(0);
	tfmeta->block_range = nullptr;

	return std::move(tfmeta);
}

void TemporaryFileMetadataManager::CreateFile(const string &filename) {

	// We can check that the file exists without locking because duckdb handles the synchronization for us(per file)
	if (file_to_temp_meta.find(filename) != file_to_temp_meta.end()) {
		file_to_temp_meta[filename]->is_active.store(true);
		return;
	}

	unique_ptr<TempFileMetadata> tfmeta = CreateTempFileMetadata(filename);

	// Since the duckdb synchronization is per file, we have to lock the shared range block allocation
	alloc_lock.lock();
	tfmeta->block_range = block_manager->AllocateBlock((tfmeta->nr_blocks * tfmeta->block_size) / lba_size);
	alloc_lock.unlock();

	tfmeta->lba_location.store(tfmeta->block_range->GetStartLBA());
	file_to_temp_meta[filename] = std::move(tfmeta);
}

idx_t TemporaryFileMetadataManager::GetLBA(const string &filename, idx_t lba_location) {
	// We assume that the file exists
	TempFileMetadata &tfmeta = *file_to_temp_meta[filename];

	return tfmeta.block_range->GetStartLBA() + lba_location;
}

void TemporaryFileMetadataManager::MoveLBALocation(const string &filename, idx_t lba_location) {
	TempFileMetadata *tfmeta = file_to_temp_meta[filename].get();

	// Use atomic compare-and-swap to update lba_location if the new location is larger
	idx_t current_lba = tfmeta->lba_location.load();
	while (lba_location > current_lba) {
		// Attempt to update the lba_location atomically
		if (tfmeta->lba_location.compare_exchange_weak(current_lba, lba_location)) {
			return; // Update successful
		}

		current_lba = tfmeta->lba_location.load(); // Reload current value
	}
}

void TemporaryFileMetadataManager::TruncateFile(const string &filename, idx_t new_size) {
	TempFileMetadata *tfmeta = file_to_temp_meta[filename].get();

	idx_t new_lba_location = new_size / 4096;

	idx_t current_lba = tfmeta->lba_location.load();
	while (current_lba > new_lba_location) {

		// Attempt to update the lba_location atomically
		if (tfmeta->lba_location.compare_exchange_weak(current_lba, new_lba_location)) {
			return; // Update successful
		}

		current_lba = tfmeta->lba_location.load(); // Reload current value
	}
}

void TemporaryFileMetadataManager::DeleteFile(const string &filename) {
	TempFileMetadata *tfmeta = file_to_temp_meta[filename].get();

	tfmeta->is_active.store(false); // Soft delete the file
}

bool TemporaryFileMetadataManager::FileExists(const string &filename) {

	if (file_to_temp_meta.find(filename) != file_to_temp_meta.end()) {
		return file_to_temp_meta[filename]->is_active.load();
	}

	return false;
}

idx_t TemporaryFileMetadataManager::GetFileSize(const string &filename) {
	TempFileMetadata *tfmeta = file_to_temp_meta[filename].get();

	return tfmeta->lba_location.load() * lba_size;
}

void TemporaryFileMetadataManager::Clear() {
	alloc_lock.lock();
	for (auto &kv : file_to_temp_meta) {
		block_manager->FreeBlock(kv.second->block_range);
	}

	file_to_temp_meta.clear();
	alloc_lock.unlock();

} // namespace duckdb

idx_t TemporaryFileMetadataManager::GetAvailableSpace() {
	idx_t available_space = lba_amount * lba_size;
	for (auto &kv : file_to_temp_meta) {
		if (kv.second->is_active.load()) {
			available_space -= kv.second->block_range->GetSizeInBytes();
		}
	}

	return available_space;
}

void TemporaryFileMetadataManager::ListFiles(const string &directory,
                                             const std::function<void(const string &, bool)> &callback) {
	for (const auto &kv : file_to_temp_meta) {
		callback(StringUtil::GetFileName(kv.first), false);
	}
}
} // namespace duckdb
