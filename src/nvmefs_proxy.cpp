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

NvmeFileSystemProxy::NvmeFileSystemProxy()
    : fs(make_uniq<NvmeFileSystem>(*this)), allocator(Allocator::DefaultAllocator()) {
}

unique_ptr<FileHandle> NvmeFileSystemProxy::OpenFile(const string &path, FileOpenFlags flags,
                                                     optional_ptr<FileOpener> opener) {

	unique_ptr<FileHandle> handle = fs->OpenFile(path, flags, opener);

	if (!metadata || !TryLoadMetadata(opener)) {
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
	uint64_t lba_start_location = GetLBA(type, handle.path, location);

	fs->Read(handle, buffer, nr_bytes, lba_start_location);
}

void NvmeFileSystemProxy::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	MetadataType type = GetMetadataType(handle.path);
	uint64_t lba_start_location = GetLBA(type, handle.path, location);
	uint64_t written_lbas = fs->WriteInternal(handle, buffer, nr_bytes, lba_start_location);
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

	int64_t lbas_written = fs->WriteInternal(handle, buffer, nr_bytes, lba_start_location);

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
	if (!metadata || !TryLoadMetadata(opener)) {
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

	Metadata meta_db {.start = 1, .end = 5001, .location = 1};
	Metadata meta_wal {.start = 5002, .end = 10002, .location = 5002};
	Metadata meta_temp {.start = 10003, .end = 15003, .location = 10003};

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

	unique_ptr<GlobalMetadata> global = make_uniq<GlobalMetadata>();

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

	// Number of locations that the number of LBAs will occupy
	uint64_t occupy = (nr_lbas + LBAS_PER_LOCATION - 1) / LBAS_PER_LOCATION;
	// Translate location count to LBAs
	uint64_t lba_occupy = occupy * LBAS_PER_LOCATION;

	switch (type) {
	case MetadataType::WAL:
		if (location >= metadata->write_ahead_log.location) {
			metadata->write_ahead_log.location = location + lba_occupy;
			write = true;
		}
		break;
	case MetadataType::TEMPORARY:
		if (location >= metadata->temporary.location) {
			metadata->temporary.location = location + lba_occupy;
			write = true;
		}
		break;
	case MetadataType::DATABASE:
		if (location >= metadata->database.location) {
			metadata->database.location = location + lba_occupy;
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

	uint64_t location_lba_position = LBAS_PER_LOCATION * location;

	switch (type) {
	case MetadataType::WAL:
		// TODO: Alignment???
		if (location_lba_position < metadata->write_ahead_log.location) {
			lba = metadata->write_ahead_log.start + location_lba_position;
		} else {
			lba = metadata->write_ahead_log.location;
		}
		break;
	case MetadataType::TEMPORARY:
		if (file_to_lba.count(filename)) {
			lba = file_to_lba[filename] + location_lba_position;
		} else {
			lba = metadata->temporary.location;
			file_to_lba[filename] = lba;
		}
		break;
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
			lba = file_to_lba[filename];
		} else {
			lba = metadata->temporary.location;
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
	case MetadataType::TEMPORARY:
		throw NotImplementedException("GetLocationLBA for temp not implemented");
		break;
	case MetadataType::DATABASE:
		lba = metadata->database.location;
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

} // namespace duckdb
