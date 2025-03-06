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

void NvmeFileSystemProxy::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	fs->Read(handle, buffer, nr_bytes, location);
}

void NvmeFileSystemProxy::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	MetadataType type = GetMetadataType(handle.path);
	uint64_t lba_start_location = GetLBA(type, handle.path, location);
	uint64_t written_lbas = fs->WriteInternal(handle, buffer, nr_bytes, lba_start_location);
	WriteMetadata(lba_start_location, written_lbas, type);
	PrintFullMetadata(*metadata);
}

unique_ptr<FileHandle> NvmeFileSystemProxy::OpenFile(const string &path, FileOpenFlags flags,
                                                     optional_ptr<FileOpener> opener) {
	if (!metadata) {
		metadata = LoadMetadata(opener);

		PrintFullMetadata(*metadata);
	}
	return fs->OpenFile(path, flags, opener);
}

bool NvmeFileSystemProxy::CanHandleFile(const string &fpath) {
	return fs->CanHandleFile(fpath);
}

unique_ptr<GlobalMetadata> NvmeFileSystemProxy::LoadMetadata(optional_ptr<FileOpener> opener) {

	idx_t bytes_to_read = sizeof(MAGIC_BYTES) + sizeof(GlobalMetadata);
	data_ptr_t buffer = allocator.AllocateData(bytes_to_read);
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ;

	unique_ptr<FileHandle> fh = fs->OpenFile(NVME_GLOBAL_METADATA_PATH, flags, opener);

	fs->Read(*fh, buffer, bytes_to_read, NVMEFS_METADATA_LOCATION);

	// Check magic bytes
	if (memcmp(buffer, MAGIC_BYTES, sizeof(MAGIC_BYTES)) != 0) {
		allocator.FreeData(buffer, bytes_to_read);
		return InitializeMetadata(opener);
	}

	unique_ptr<GlobalMetadata> global = make_uniq<GlobalMetadata>(GlobalMetadata {});
	memcpy(&global, (buffer + sizeof(MAGIC_BYTES)), sizeof(GlobalMetadata));

	allocator.FreeData(buffer, bytes_to_read);

	return std::move(global);
}

void NvmeFileSystemProxy::WriteMetadata(uint64_t location, uint64_t nr_lbas, MetadataType type) {
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
		idx_t bytes_to_write = sizeof(MAGIC_BYTES) + sizeof(GlobalMetadata);
		idx_t medata_location = 0;

		data_ptr_t buffer = allocator.AllocateData(bytes_to_write);

		memcpy(buffer, MAGIC_BYTES, sizeof(MAGIC_BYTES));
		memcpy(buffer + sizeof(MAGIC_BYTES), &metadata, sizeof(GlobalMetadata));

		FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE;

		unique_ptr<FileHandle> fh = fs->OpenFile(NVME_GLOBAL_METADATA_PATH, flags);
		fs->Write(*fh, buffer, bytes_to_write, medata_location);

		allocator.FreeData(buffer, bytes_to_write);
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

unique_ptr<GlobalMetadata> NvmeFileSystemProxy::InitializeMetadata(optional_ptr<FileOpener> opener) {
	// Create buffer
	// insert magic bytes
	// insert metadata
	// 	- We know size of db and WAL metadata and partly temp
	// 	- We do not know the size of the file mapping field in temp
	// 		- Should this be constant based on the size of directory?
	//		- Dynamic?
	// Example:
	//  1 GB temp data -> x files -> map that supports x files total (this is the size)

	idx_t bytes_to_write = sizeof(MAGIC_BYTES) + sizeof(GlobalMetadata);
	idx_t medata_location = 0;

	data_ptr_t buffer = allocator.AllocateData(bytes_to_write);

	Metadata meta_db {.start = 1, .end = 5001, .location = 1};
	Metadata meta_wal {.start = 5002, .end = 10002, .location = 5002};
	Metadata meta_temp {.start = 10003, .end = 15003, .location = 10003};

	unique_ptr<GlobalMetadata> global = make_uniq<GlobalMetadata>(GlobalMetadata {});

	global->database = meta_db;
	global->temporary = meta_temp;
	global->write_ahead_log = meta_wal;

	memcpy(buffer, MAGIC_BYTES, sizeof(MAGIC_BYTES));
	memcpy(buffer + sizeof(MAGIC_BYTES), global.get(), sizeof(GlobalMetadata));

	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE;

	unique_ptr<FileHandle> fh = fs->OpenFile(NVME_GLOBAL_METADATA_PATH, flags, opener);
	fs->Write(*fh, buffer, bytes_to_write, medata_location);

	allocator.FreeData(buffer, bytes_to_write);

	return std::move(global);
}

int64_t NvmeFileSystemProxy::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	return fs->Read(handle, buffer, nr_bytes);
}
int64_t NvmeFileSystemProxy::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	return fs->Write(handle, buffer, nr_bytes);
}

} // namespace duckdb
