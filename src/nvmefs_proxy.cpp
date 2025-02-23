#include "nvmefs_proxy.hpp"

#include <iostream>

namespace duckdb {

const char MAGIC_BYTES[] = "NVMEFS";

NvmeFileSystemProxy::NvmeFileSystemProxy() : fs(make_uniq<NvmeFileSystem>(*this)) {
	// metadata = LoadMetadata();

	// // print for debug
	// std::cout << "this is metadata db - start: " << metadata.database.start << " end: " << metadata.database.end
	//           << " loc: " << metadata.database.location << std::endl;
	// std::cout << "this is metadata wal - start: " << metadata.write_ahead_log.start
	//           << " end: " << metadata.write_ahead_log.end << " loc: " << metadata.write_ahead_log.location <<
	//           std::endl;
	// std::cout << "this is metadata temp - start: " << metadata.temporary.start << " end: " << metadata.temporary.end
	//           << " loc: " << metadata.temporary.location << std::endl;
}

void NvmeFileSystemProxy::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	fs->Read(handle, buffer, nr_bytes, location);
}

void NvmeFileSystemProxy::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	fs->Write(handle, buffer, nr_bytes, location);
}

unique_ptr<FileHandle> NvmeFileSystemProxy::OpenFile(const string &path, FileOpenFlags flags,
                                                     optional_ptr<FileOpener> opener) {
	return fs->OpenFile(path, flags, opener);
}

bool NvmeFileSystemProxy::CanHandleFile(const string &fpath) {
	return fs->CanHandleFile(fpath);
}

GlobalMetadata NvmeFileSystemProxy::LoadMetadata() {
	// Open file check magic bytes
	// if no magic bytes/not equal
	// InitializeMetadata()
	// else proceed ...
	idx_t bytes_to_read = sizeof(MAGIC_BYTES) + sizeof(GlobalMetadata);
	idx_t metadata_location = 0;
	uint8_t *buf = new uint8_t[bytes_to_read];
	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ;

	unique_ptr<FileHandle> fh = fs->OpenFile(NVME_GLOBAL_METADATA_PATH, flags);

	fs->Read(*fh, buf, bytes_to_read, metadata_location);

	// Check magic bytes
	if (memcmp(buf, MAGIC_BYTES, sizeof(MAGIC_BYTES)) != 0) {
		delete[] buf;
		return InitializeMetadata();
	}

	GlobalMetadata global;
	memcpy(&global, (buf + sizeof(MAGIC_BYTES)), sizeof(GlobalMetadata));

	delete[] buf;
	return global;
}

void NvmeFileSystemProxy::WriteMetadata() {
}

uint64_t NvmeFileSystemProxy::GetLBA(MetadataType type, std::string filename) {
	return 0;
}

GlobalMetadata NvmeFileSystemProxy::InitializeMetadata() {
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

	uint8_t *buf = new uint8_t[bytes_to_write];

	Metadata meta_db {.start = 1, .end = 5001, .location = 1};
	Metadata meta_wal {.start = 5002, .end = 10002, .location = 5002};
	Metadata meta_temp {.start = 10003, .end = 15003, .location = 10003};

	GlobalMetadata global {.database = meta_db, .write_ahead_log = meta_wal, .temporary = meta_temp};

	memcpy(buf, MAGIC_BYTES, sizeof(MAGIC_BYTES));
	memcpy(buf + sizeof(MAGIC_BYTES), &global, sizeof(GlobalMetadata));

	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE;

	unique_ptr<FileHandle> fh = fs->OpenFile(NVME_GLOBAL_METADATA_PATH, flags);
	fs->Write(*fh, buf, bytes_to_write, medata_location);

	delete[] buf;

	return global;
}

} // namespace duckdb
