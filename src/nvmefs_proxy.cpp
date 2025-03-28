#include "nvmefs_proxy.hpp"

#include "duckdb/main/extension_util.hpp"
#include "duckdb/common/string_util.hpp"

#include <iostream>

namespace duckdb {

const char MAGIC_BYTES[] = "NVMEFS";

#ifdef DEBUG
void PrintMetadata(Metadata &meta, string name) {
	printf("Metadata for %s\n", name.c_str());
	std::cout << "start: " << meta.start << " end: " << meta.end << " loc: " << meta.location << std::endl;
}
void PrintDebug(string debug) {
	std::cout << debug << std::endl;
}
void PrintFullMetadata(GlobalMetadata &metadata) {
	PrintMetadata(metadata.database, "database");
	PrintMetadata(metadata.write_ahead_log, "write_ahead_log");
	PrintMetadata(metadata.temporary, "temporary");
}
#else
void PrintMetadata(Metadata &meta, string name) {
}
void PrintDebug(string debug) {
}
void PrintFullMetadata(GlobalMetadata &metadata) {
}
#endif

// TODO: Should this constructor be removed?
NvmeFileSystemProxy::NvmeFileSystemProxy()
    : fs(make_uniq<NvmeFileSystem>(*this)), allocator(Allocator::DefaultAllocator()) {
}

NvmeFileSystemProxy::NvmeFileSystemProxy(NvmeConfig config)
    : fs(make_uniq<NvmeFileSystem>(*this, config.device_path, config.plhdls)), allocator(Allocator::DefaultAllocator()),
      max_temp_size(config.max_temp_size), max_wal_size(config.max_wal_size) {
	geometry = fs->GetDeviceGeometry();
}

unique_ptr<FileHandle> NvmeFileSystemProxy::OpenFile(const string &path, FileOpenFlags flags,
                                                     optional_ptr<FileOpener> opener) {

	unique_ptr<FileHandle> handle = fs->OpenFile(path, flags, opener);

	if (!TryLoadMetadata(opener)) {
		if (GetMetadataType(path) != MetadataType::DATABASE) {
			throw IOException("No attached database");
		} else {
			InitializeMetadata(*handle.get(), path);
		}
	}
	return move(handle);
}

void NvmeFileSystemProxy::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	MetadataType type = GetMetadataType(handle.path);
	int64_t cursor_offset = fs->SeekPosition(handle);
	location += cursor_offset;

	uint64_t lba_start_location = GetLBA(type, handle.path, location);

	// Get the offset of bytes within the block
	int16_t in_block_offset = location % NVME_BLOCK_SIZE;
	PrintDebug("Read with offset: " + std::to_string(in_block_offset));

	fs->ReadInternal(handle, buffer, nr_bytes, lba_start_location, in_block_offset);
}

void NvmeFileSystemProxy::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	MetadataType type = GetMetadataType(handle.path);
	int64_t cursor_offset = fs->SeekPosition(handle);
	location += cursor_offset;

	uint64_t lba_start_location = GetLBA(type, handle.path, location);
	uint16_t in_block_offset = location % NVME_BLOCK_SIZE;
	PrintDebug("Write with offset: " + std::to_string(in_block_offset));

	uint64_t written_lbas = fs->WriteInternal(handle, buffer, nr_bytes, lba_start_location, in_block_offset);
	UpdateMetadata(handle, lba_start_location, written_lbas, type);
	PrintFullMetadata(*metadata);
}

int64_t NvmeFileSystemProxy::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	MetadataType meta_type = GetMetadataType(handle.path);
	uint64_t lba_start_location = GetStartLBA(meta_type, handle.path);

	data_ptr_t temp_buf = allocator.AllocateData(nr_bytes);

	fs->Read(handle, temp_buf, nr_bytes, lba_start_location);

	memcpy(buffer, temp_buf, nr_bytes);
	allocator.FreeData(temp_buf, nr_bytes);

	return nr_bytes;
}

int64_t NvmeFileSystemProxy::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	MetadataType meta_type = GetMetadataType(handle.path);
	uint64_t lba_start_location = GetStartLBA(meta_type, handle.path);

	int64_t lbas_written = fs->WriteInternal(handle, buffer, nr_bytes, lba_start_location, 0);

	PrintDebug("Number of bytes: " + std::to_string(nr_bytes) + "\n");
	UpdateMetadata(handle, lba_start_location, lbas_written, meta_type);
	PrintFullMetadata(*metadata);

	return nr_bytes;
}

bool NvmeFileSystemProxy::CanHandleFile(const string &fpath) {
	return fs->CanHandleFile(fpath);
}

bool NvmeFileSystemProxy::FileExists(const string &filename, optional_ptr<FileOpener> opener) {

	// TODO: Add statement to check if the file is a db file in order to init/load metadata
	//		 in this function
	if (!TryLoadMetadata(opener)) {
		return false;
	}

	MetadataType type = GetMetadataType(filename);
	bool exists = false;
	string path_no_ext = StringUtil::GetFileStem(filename);
	string db_path_no_ext = StringUtil::GetFileStem(metadata->db_path);

	switch (type) {

	case WAL:
		/*
		    Intentional fall-through. Need to remove the '.wal' and db ext
		    before evaluating if the file exists.

		    Example:
		        string filename = "test.db.wal"

		        // After two calls to GetFileStem would be: "test"

		*/
		path_no_ext = StringUtil::GetFileStem(path_no_ext);

	case DATABASE:
		if (StringUtil::Equals(path_no_ext.data(), db_path_no_ext.data())) {
			uint64_t start_lba = GetStartLBA(type, filename);
			uint64_t location_lba = GetLocationLBA(type, filename);

			if ((location_lba - start_lba) > 0) {
				exists = true;
			}
		} else {
			throw IOException("Not possible to have multiple databases");
		}
		break;
	case TEMPORARY:
		if (file_to_lba.count(filename)) {
			exists = true;
		}
		break;
	default:
		throw IOException("No such metadatatype");
		break;
	}

	return exists;
}

bool NvmeFileSystemProxy::TryLoadMetadata(optional_ptr<FileOpener> opener) {
	if (metadata) {
		return true;
	}

	unique_ptr<FileHandle> handle = fs->OpenFile(NVME_GLOBAL_METADATA_PATH, FileOpenFlags::FILE_FLAGS_READ, opener);

	unique_ptr<GlobalMetadata> global = ReadMetadata(*handle.get());
	if (global) {
		metadata = std::move(global);
		return true;
	}
	return false;
}

void NvmeFileSystemProxy::InitializeMetadata(FileHandle &handle, string path) {
	// Create buffer
	// insert magic bytes
	// insert metadata
	// 	- We know size of db and WAL metadata and partly temp
	// 	- We do not know the size of the file mapping field in temp
	// 		- Should this be constant based on the size of directory?
	//		- Dynamic?
	// Example:
	//  1 GB temp data -> x files -> map that supports x files total (this is the size)

	uint64_t temp_start = (geometry->lba_count - 1) - (max_temp_size / geometry->lba_size);

	uint64_t wal_lba_count = max_wal_size / geometry->lba_size;
	uint64_t wal_start = (temp_start - 1) - max_wal_size;

	Metadata meta_temp {.start = temp_start, .end = geometry->lba_count - 1, .location = temp_start};
	Metadata meta_wal {.start = wal_start, .end = temp_start - 1, .location = wal_start};
	Metadata meta_db {.start = 1,
	                  .end = wal_start - 1,
	                  .location = 1}; // 1 is the first lba due to lba 0 being allocated for device metadata

	unique_ptr<GlobalMetadata> global = make_uniq<GlobalMetadata>(GlobalMetadata {});

	if (path.length() > 100) {
		throw IOException("Database name is too long.");
	}

	global->database = meta_db;
	global->temporary = meta_temp;
	global->write_ahead_log = meta_wal;

	global->db_path_size = path.length();
	strncpy(global->db_path, path.data(), path.length());
	global->db_path[100] = '\0';

	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE;

	unique_ptr<MetadataFileHandle> fh = fs->OpenMetadataFile(handle, NVME_GLOBAL_METADATA_PATH, flags);

	WriteMetadata(*fh.get(), global.get());

	metadata = std::move(global);
}

unique_ptr<GlobalMetadata> NvmeFileSystemProxy::ReadMetadata(FileHandle &handle) {

	idx_t bytes_to_read = sizeof(MAGIC_BYTES) + sizeof(GlobalMetadata);
	data_ptr_t buffer = allocator.AllocateData(bytes_to_read);
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ;

	unique_ptr<MetadataFileHandle> fh = fs->OpenMetadataFile(handle, NVME_GLOBAL_METADATA_PATH, flags);

	fs->Read(*fh, buffer, bytes_to_read, NVMEFS_METADATA_LOCATION);

	unique_ptr<GlobalMetadata> global = nullptr;

	// Check magic bytes
	if (memcmp(buffer, MAGIC_BYTES, sizeof(MAGIC_BYTES)) == 0) {
		global = make_uniq<GlobalMetadata>(GlobalMetadata {});
		memcpy(global.get(), (buffer + sizeof(MAGIC_BYTES)), sizeof(GlobalMetadata));
	}

	allocator.FreeData(buffer, bytes_to_read);

	return std::move(global);
}

void NvmeFileSystemProxy::WriteMetadata(MetadataFileHandle &handle, GlobalMetadata *global) {
	idx_t bytes_to_write = sizeof(MAGIC_BYTES) + sizeof(GlobalMetadata);
	idx_t medata_location = 0;

	data_ptr_t buffer = allocator.AllocateData(bytes_to_write);

	memcpy(buffer, MAGIC_BYTES, sizeof(MAGIC_BYTES));
	memcpy(buffer + sizeof(MAGIC_BYTES), global, sizeof(GlobalMetadata));

	fs->Write(handle, buffer, bytes_to_write, medata_location);

	allocator.FreeData(buffer, bytes_to_write);
}

void NvmeFileSystemProxy::UpdateMetadata(FileHandle &handle, uint64_t location, uint64_t nr_lbas, MetadataType type) {
	bool write = false;

	switch (type) {
	case MetadataType::WAL:
		if (location >= metadata->write_ahead_log.location) {
			metadata->write_ahead_log.location = location + nr_lbas;
			write = true;
		}
		break;
	case MetadataType::TEMPORARY:
		if (location >= metadata->temporary.location) {
			metadata->temporary.location = location + nr_lbas;
			write = true;
			TemporaryFileMetadata tfmeta_tmp = file_to_lba[handle.path];
			file_to_lba[handle.path] = {tfmeta_tmp.start, metadata->temporary.location - 1};
		}
		break;
	case MetadataType::DATABASE:
		if (location >= metadata->database.location) {
			metadata->database.location = location + nr_lbas;
			write = true;
		}
		break;
	default:
		throw InvalidInputException("no such metadatatype");
	}
	if (write) {
		FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_WRITE;
		unique_ptr<MetadataFileHandle> fh = fs->OpenMetadataFile(handle, NVME_GLOBAL_METADATA_PATH, flags);
		WriteMetadata(*fh, metadata.get());
	}
}

MetadataType NvmeFileSystemProxy::GetMetadataType(string path) {
	PrintDebug("Determining metadatatype for path:");
	PrintDebug(path);
	if (StringUtil::Contains(path, ".wal")) {
		PrintDebug("It was Write ahead log");
		return MetadataType::WAL;
	} else if (StringUtil::Contains(path, "/tmp")) {
		PrintDebug("It was the temporary");
		return MetadataType::TEMPORARY;
	} else {
		PrintDebug("It was the database");
		return MetadataType::DATABASE;
	}
}

uint64_t NvmeFileSystemProxy::GetLBA(MetadataType type, string filename, idx_t location) {
	// TODO: for WAL and temp ensure that it can fit in range
	// otherwise increase size + update mapping to temp files for temp type
	uint64_t lba {};

	uint64_t location_lba_position = location / NVME_BLOCK_SIZE;

	switch (type) {
	case MetadataType::WAL:
		// TODO: Alignment???
		if (location_lba_position < metadata->write_ahead_log.location) {
			lba = metadata->write_ahead_log.start + location_lba_position;
		} else {
			lba = metadata->write_ahead_log.location;
		}
		break;
	case MetadataType::TEMPORARY: {
		TemporaryFileMetadata tfmeta;
		if (file_to_lba.count(filename)) {
			tfmeta = file_to_lba[filename];
			lba = tfmeta.start + location_lba_position;
		} else {
			lba = metadata->temporary.location;
			tfmeta = {.start = lba, .end = lba};
			file_to_lba[filename] = tfmeta;
		}
	} break;
	case MetadataType::DATABASE:
		lba = metadata->database.start + location_lba_position;
		break;
	default:
		throw InvalidInputException("no such metadatatype");
	}

	return lba;
}

uint64_t NvmeFileSystemProxy::GetStartLBA(MetadataType type, string filename) {
	uint64_t lba {};

	switch (type) {
	case MetadataType::WAL:
		// TODO: Alignment???
		lba = metadata->write_ahead_log.start;
		break;
	case MetadataType::TEMPORARY:
		if (file_to_lba.count(filename)) {
			TemporaryFileMetadata tfmeta = file_to_lba[filename];
			lba = tfmeta.start;
		} else {
			lba = metadata->temporary.location;
			TemporaryFileMetadata tfmeta = {.start = lba, .end = lba};
			file_to_lba[filename] = tfmeta;
		}
		break;
	case MetadataType::DATABASE:
		lba = metadata->database.start;
		break;
	default:
		throw InvalidInputException("no such metadatatype");
	}

	return lba;
}

uint64_t NvmeFileSystemProxy::GetLocationLBA(MetadataType type, string filename) {
	uint64_t lba {};

	switch (type) {
	case MetadataType::WAL:
		lba = metadata->write_ahead_log.location;
		break;
	case MetadataType::TEMPORARY: {
		TemporaryFileMetadata tfmeta = file_to_lba[filename];
		// Consider temp file lba 0 to 4. end = 4. proper size of tempfile is 5 lbas, so end+1
		lba = tfmeta.end + 1;
	} break;
	case MetadataType::DATABASE:
		lba = metadata->database.location;
		break;
	default:
		throw InvalidInputException("no such metadatatype");
	}

	return lba;
}

uint64_t NvmeFileSystemProxy::GetEndLBA(MetadataType type, string filename) {
	uint64_t lba {};

	switch (type) {
	case MetadataType::WAL:
		lba = metadata->write_ahead_log.end;
		break;
	case MetadataType::TEMPORARY: {
		TemporaryFileMetadata tfmeta = file_to_lba[filename];
		lba = tfmeta.end;
	} break;
	case MetadataType::DATABASE:
		lba = metadata->database.end;
		break;
	default:
		throw InvalidInputException("no such metadatatype");
	}

	return lba;
}

int64_t NvmeFileSystemProxy::GetFileSize(FileHandle &handle) {

	D_ASSERT(this->metadata);

	MetadataType type = GetMetadataType(handle.path);
	uint64_t start_lba = GetStartLBA(type, handle.path);
	uint64_t location_lba = GetLocationLBA(type, handle.path);

	return (location_lba - start_lba) *
	       NVME_BLOCK_SIZE; // TODO: NVME_BLOCK_SIZE should be changed. We should get it from the filehandle
}

void NvmeFileSystemProxy::FileSync(FileHandle &handle) {
	// This should just be empty. We do not need to sync to disk since we write directly to disk
}

bool NvmeFileSystemProxy::OnDiskFile(FileHandle &handle) {
	// This filesystem only interacts with the disk, hence file will always be on disk
	return true;
}

bool NvmeFileSystemProxy::DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) {

	if (!TryLoadMetadata(opener)) {
		return false;
	}

	// We do not need to look further. The directory is created when the metadata is created
	return true;
}

void NvmeFileSystemProxy::RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	// Only supported "directory" is the temporary directory
	if (GetMetadataType(directory) == MetadataType::TEMPORARY) {
		file_to_lba.clear();
	} else {
		throw IOException("Cannot delete unknown directory type");
	}
}

void NvmeFileSystemProxy::CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	if (!TryLoadMetadata(opener)) {
		throw IOException("Not possible to create directory without an database");
	}
	// All necessary directories are already create on the device if an database exists.
	// i.e. the temporary directory
}

void NvmeFileSystemProxy::RemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	MetadataType type = GetMetadataType(filename);

	switch (type) {
	case WAL:
		// Reset the location poitner (next lba to write to) to the start effectively removing the wal
		metadata->write_ahead_log.location = metadata->write_ahead_log.start;
		break;

	case TEMPORARY:
		// TODO: how do we determine if we need to move the temp metadata location pointer
		// and what about fragmentation? is it even possible to use ringbuffer technique?
		file_to_lba.erase(filename);
		break;
	default:
		// No other files to delete - we only have the database file, temporary files and the write_ahead_log
		break;
	}
}

void NvmeFileSystemProxy::Seek(FileHandle &handle, idx_t location) {

	D_ASSERT(location % NVME_BLOCK_SIZE == 0);
	auto type = GetMetadataType(handle.path);

	uint64_t end = (GetEndLBA(type, handle.path) - GetStartLBA(type, handle.path)) * NVME_BLOCK_SIZE;

	if (location > end) {
		throw IOException("Seek location out of bounds");
	}

	fs->Seek(handle, location);
}

idx_t NvmeFileSystemProxy::SeekPosition(FileHandle &handle) {

	return fs->SeekPosition(handle);
}

} // namespace duckdb
