#include "nvmefs.hpp"

namespace duckdb {
NvmeFileHandle::NvmeFileHandle(FileSystem &file_system, string path, FileOpenFlags flags)
    : FileHandle(file_system, path, flags), cursor_offset(0) {
}

void NvmeFileHandle::Read(void *buffer, idx_t nr_bytes, idx_t location) {
	file_system.Read(*this, buffer, nr_bytes, location);
}

void NvmeFileHandle::Write(void *buffer, idx_t nr_bytes, idx_t location) {
	file_system.Write(*this, buffer, nr_bytes, location);
}

idx_t NvmeFileHandle::GetFileSize() {
	return file_system.GetFileSize(*this);
}

void NvmeFileHandle::Sync() {
	file_system.FileSync(*this);
}

void NvmeFileHandle::Close() {
}

unique_ptr<CmdContext> NvmeFileHandle::PrepareWriteCommand(idx_t nr_bytes, idx_t start_lba, idx_t offset) {
	unique_ptr<NvmeCmdContext> nvme_cmd_ctx = make_uniq<NvmeCmdContext>();
	nvme_cmd_ctx->nr_bytes = nr_bytes;
	nvme_cmd_ctx->filepath = path;
	nvme_cmd_ctx->offset = offset;
	nvme_cmd_ctx->start_lba = start_lba;
	nvme_cmd_ctx->nr_lbas = CalculateRequiredLBACount(nr_bytes);

	return std::move(nvme_cmd_ctx);
}

unique_ptr<CmdContext> NvmeFileHandle::PrepareReadCommand(idx_t nr_bytes, idx_t start_lba, idx_t offset) {
	unique_ptr<NvmeCmdContext> nvme_cmd_ctx = make_uniq<NvmeCmdContext>();
	nvme_cmd_ctx->nr_bytes = nr_bytes;
	nvme_cmd_ctx->filepath = path;
	nvme_cmd_ctx->offset = offset;
	nvme_cmd_ctx->start_lba = start_lba;
	nvme_cmd_ctx->nr_lbas = CalculateRequiredLBACount(nr_bytes);

	return std::move(nvme_cmd_ctx);
}

idx_t NvmeFileHandle::CalculateRequiredLBACount(idx_t nr_bytes) {
	NvmeFileSystem &nvmefs = file_system.Cast<NvmeFileSystem>();
	DeviceGeometry geo = nvmefs.GetDevice().GetDeviceGeometry();
	idx_t lba_size = geo.lba_size;
	return (nr_bytes + lba_size - 1) / lba_size;
}

void NvmeFileHandle::SetFilePointer(idx_t location) {
	cursor_offset = location;
}

idx_t NvmeFileHandle::GetFilePointer() {
	return cursor_offset;
}

////////////////////////////////////////

std::recursive_mutex NvmeFileSystem::temp_lock;

NvmeFileSystem::NvmeFileSystem(NvmeConfig config)
    : allocator(Allocator::DefaultAllocator()),
      device(make_uniq<NvmeDevice>(config.device_path, config.plhdls, config.backend, config.async)),
      max_temp_size(config.max_temp_size), max_wal_size(config.max_wal_size) {
}

NvmeFileSystem::NvmeFileSystem(NvmeConfig config, unique_ptr<Device> device)
    : allocator(Allocator::DefaultAllocator()), device(std::move(device)), max_temp_size(config.max_temp_size),
      max_wal_size(config.max_wal_size) {
}

NvmeFileSystem::~NvmeFileSystem() {
	WriteMetadata(*metadata);
}

unique_ptr<FileHandle> NvmeFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                                optional_ptr<FileOpener> opener) {
	bool internal = StringUtil::Equals(NVMEFS_GLOBAL_METADATA_PATH.data(), path.data());
	if (!internal && !TryLoadMetadata()) {
		if (GetMetadataType(path) != MetadataType::DATABASE) {
			throw IOException("No database is attached");
		} else {
			InitializeMetadata(path);
		}
	}
	unique_ptr<FileHandle> handle = make_uniq<NvmeFileHandle>(*this, path, flags);
	return std::move(handle);
}

void NvmeFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	NvmeFileHandle &fh = handle.Cast<NvmeFileHandle>();
	DeviceGeometry geo = device->GetDeviceGeometry();

	idx_t cursor_offset = SeekPosition(handle);
	location += cursor_offset;
	idx_t nr_lbas = fh.CalculateRequiredLBACount(nr_bytes);
	idx_t start_lba = GetLBA(handle.path, nr_bytes, location, nr_lbas);
	idx_t in_block_offset = location % geo.lba_size;
	unique_ptr<CmdContext> cmd_ctx = fh.PrepareReadCommand(nr_bytes, start_lba, in_block_offset);

	if (!IsLBAInRange(handle.path, start_lba, cmd_ctx->nr_lbas)) {
		throw IOException("Read out of range");
	}

	device->Read(buffer, *cmd_ctx);
}

void NvmeFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	NvmeFileHandle &fh = handle.Cast<NvmeFileHandle>();
	DeviceGeometry geo = device->GetDeviceGeometry();

	idx_t cursor_offset = SeekPosition(handle);
	location += cursor_offset;
	idx_t nr_lbas = fh.CalculateRequiredLBACount(nr_bytes);
	idx_t start_lba = GetLBA(fh.path, nr_bytes, location, nr_lbas);
	idx_t in_block_offset = location % geo.lba_size;
	unique_ptr<CmdContext> cmd_ctx = fh.PrepareWriteCommand(nr_bytes, start_lba, in_block_offset);

	if (!IsLBAInRange(handle.path, start_lba, cmd_ctx->nr_lbas)) {
		throw IOException("Read out of range");
	}

	idx_t written_lbas = device->Write(buffer, *cmd_ctx);
	UpdateMetadata(*cmd_ctx);
}

int64_t NvmeFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	Read(handle, buffer, nr_bytes, 0);
	return nr_bytes;
}

int64_t NvmeFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	Write(handle, buffer, nr_bytes, 0);
	return nr_bytes;
}

bool NvmeFileSystem::CanHandleFile(const string &fpath) {
	return StringUtil::StartsWith(fpath, NVMEFS_PATH_PREFIX);
}

bool NvmeFileSystem::FileExists(const string &filename, optional_ptr<FileOpener> opener) {
	if (!TryLoadMetadata()) {
		return false;
	}

	MetadataType type = GetMetadataType(filename);
	string path_no_ext = StringUtil::GetFileStem(filename);
	string db_path_no_ext = StringUtil::GetFileStem(metadata->db_path);

	bool exists = false;

	switch (type) {

	case WAL:
		/*
		    Need to remove the '.wal' and db ext before evaluating if the file exists.
		    Example:
		        string filename = "test.db.wal"
		        // After two calls to GetFileStem would be: "test"
		*/
		path_no_ext = StringUtil::GetFileStem(path_no_ext);
		if (StringUtil::Equals(path_no_ext.data(), db_path_no_ext.data())) {
			exists = true;
		}
		break;
	case DATABASE:
		if (StringUtil::Equals(path_no_ext.data(), db_path_no_ext.data())) {
			idx_t start_lba = metadata->db_start;
			idx_t location_lba = db_location.load();

			if ((location_lba - start_lba) > 0) {
				exists = true;
			}
		} else {
			throw IOException("Not possible to have multiple databases");
		}
		break;
	case TEMPORARY:
		temp_lock.lock();
		if (file_to_temp_meta.count(filename)) {
			exists = true;
		}
		temp_lock.unlock();
		break;
	default:
		throw IOException("No such metadata type");
		break;
	}
	return exists;
}

int64_t NvmeFileSystem::GetFileSize(FileHandle &handle) {
	DeviceGeometry geo = device->GetDeviceGeometry();
	NvmeFileHandle &fh = handle.Cast<NvmeFileHandle>();
	MetadataType type = GetMetadataType(fh.path);

	idx_t nr_lbas {};
	switch (type) {
	case MetadataType::DATABASE:
		nr_lbas = db_location.load() - metadata->db_start;
		break;
	case MetadataType::TEMPORARY: {
		temp_lock.lock();
		TemporaryFileMetadata tfmeta = file_to_temp_meta[fh.path];
		nr_lbas = (tfmeta.block_size * tfmeta.block_map.size()) / geo.lba_size;
		temp_lock.unlock();
		break;
	}
	case MetadataType::WAL:
		nr_lbas = wal_location.load() - metadata->wal_start;
		break;
	default:
		throw InvalidInputException("Unknown metadata type!");
		break;
	}
	return nr_lbas * geo.lba_size;
}

void NvmeFileSystem::FileSync(FileHandle &handle) {
	WriteMetadata(*metadata);
	// No need for sync. All writes are directly to disk.
}

bool NvmeFileSystem::OnDiskFile(FileHandle &handle) {
	// No remote accesses to disks. We only interact with physical device, i.e. always disk "files".
	return true;
}

void NvmeFileSystem::Truncate(FileHandle &handle, int64_t new_size) {
	NvmeFileHandle &nvme_handle = handle.Cast<NvmeFileHandle>();
	int64_t current_size = GetFileSize(nvme_handle);

	if (new_size <= current_size) {
		MetadataType type = GetMetadataType(nvme_handle.path);
		idx_t new_lba_location = nvme_handle.CalculateRequiredLBACount(new_size);

		switch (type) {
		case MetadataType::WAL:
			wal_location.store(metadata->wal_start + new_lba_location);
			break;
		case MetadataType::DATABASE:
			db_location.store(metadata->db_start + new_lba_location);
			break;
		case MetadataType::TEMPORARY: {
			temp_lock.lock();
			TemporaryFileMetadata tfmeta = file_to_temp_meta[nvme_handle.path];

			idx_t to_block_index = new_size / tfmeta.block_size;
			idx_t from_block_index = tfmeta.block_map.size();

			for (idx_t i = from_block_index; i > to_block_index; i--) {
				idx_t block_index = i - 1;
				TemporaryBlock *block = tfmeta.block_map[block_index];
				temp_block_manager->FreeBlock(block);
				tfmeta.block_map.erase(block_index);
			}

			file_to_temp_meta[nvme_handle.path] = tfmeta;
			temp_lock.unlock();
		} break;
		default:
			throw InvalidInputException("Unknown metadata type");
			break;
		}
	} else {
		throw InvalidInputException("new_size is bigger than the current file size.");
	}
}

bool NvmeFileSystem::DirectoryExists(const string &directory, optional_ptr<FileOpener> opener) {
	// The directory exists if metadata exists
	if (TryLoadMetadata()) {
		return true;
	}
	return false;
}

void NvmeFileSystem::RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	// We only support removal of temporary directory
	MetadataType type = GetMetadataType(directory);
	if (type == MetadataType::TEMPORARY) {
		temp_lock.unlock();
		file_to_temp_meta.clear();
		temp_lock.lock();
	} else {
		throw IOException("Cannot delete unknown directory");
	}
}

void NvmeFileSystem::CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) {
	// All necessary directories (i.e. tmp and main folder) is already created
	// if metadata is present
	if (!TryLoadMetadata()) {
		throw IOException("No directories can exist when there is no metadata");
	}
}

void NvmeFileSystem::RemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	MetadataType type = GetMetadataType(filename);

	switch (type) {
	case WAL:
		// Reset the location poitner (next lba to write to) to the start effectively removing the wal
		wal_location.store(metadata->wal_start);
		break;

	case TEMPORARY: {
		temp_lock.lock();
		TemporaryFileMetadata tfmeta = file_to_temp_meta[filename];
		for (const auto &kv : tfmeta.block_map) {
			temp_block_manager->FreeBlock(kv.second);
		}
		file_to_temp_meta.erase(filename);
		temp_lock.unlock();
	} break;
	default:
		// No other files to delete - we only have the database file, temporary files and the write_ahead_log
		break;
	}
}

void NvmeFileSystem::Seek(FileHandle &handle, idx_t location) {
	NvmeFileHandle &nvme_handle = handle.Cast<NvmeFileHandle>();
	DeviceGeometry geo = device->GetDeviceGeometry();
	// We only support seek to start of an LBA block
	D_ASSERT(location % geo.lba_size == 0);

	// The order of the LBA ranges is:
	// Database, Write-Ahead Log, Temporary
	MetadataType type = GetMetadataType(nvme_handle.path);
	idx_t max_seek_bound = 0;
	switch (type) {
	case WAL:
		// Reset the location poitner (next lba to write to) to the start effectively removing the wal
		max_seek_bound = ((metadata->tmp_start-1) - metadata->wal_start) * geo.lba_size;
		break;
	case DATABASE:
		max_seek_bound = ((metadata->wal_start-1) - metadata->db_start) * geo.lba_size;
		break;
	case TEMPORARY: {
		temp_lock.lock();
		TemporaryFileMetadata tfmeta = file_to_temp_meta[nvme_handle.path];
		max_seek_bound = tfmeta.block_size * tfmeta.block_map.size();
		temp_lock.unlock();
	} break;
	default:
		// No other files to delete - we only have the database file, temporary files and the write_ahead_log
		break;
	}

	if (location >= max_seek_bound) {
		throw IOException("Seek location is out of bounds");
	}

	nvme_handle.SetFilePointer(location);
}

void NvmeFileSystem::Reset(FileHandle &handle) {
	NvmeFileHandle &fh = handle.Cast<NvmeFileHandle>();
	fh.SetFilePointer(0);
}

idx_t NvmeFileSystem::SeekPosition(FileHandle &handle) {
	return handle.Cast<NvmeFileHandle>().GetFilePointer();
}

bool NvmeFileSystem::ListFiles(const string &directory,
	const std::function<void(const string &, bool)> &callback,
	FileOpener *opener) {
		bool dir = false;
		if (StringUtil::Equals(directory.data(), NVMEFS_PATH_PREFIX.data())) {
			const string db_filename_no_ext = StringUtil::GetFileStem(metadata->db_path);
			const string db_filename_with_ext = db_filename_no_ext + ".db";
			const string db_wal = db_filename_with_ext + ".wal";
			const string db_tmp = "/tmp";

			callback(db_filename_with_ext, false);
			callback(db_tmp, true);
			callback(db_wal, false);

			dir = true;
		} else if (StringUtil::Equals(directory.data(), NVMEFS_TMP_DIR_PATH.data())) {
			temp_lock.lock();
			for(const auto& kv : file_to_temp_meta) {
				callback(StringUtil::GetFileName(kv.first), false);
			}
			temp_lock.unlock();
			dir = true;
		}
		return dir;
}

optional_idx NvmeFileSystem::GetAvailableDiskSpace(const string &path){
	DeviceGeometry geo = device->GetDeviceGeometry();
	const string db_filename_no_ext = StringUtil::GetFileStem(metadata->db_path);
	const string db_filepath = NVMEFS_PATH_PREFIX + db_filename_no_ext + ".db";
	const string wal_filepath = db_filepath + ".wal";

	optional_idx remaining;

	if (StringUtil::Equals(path.data(), NVMEFS_PATH_PREFIX.data())){
		idx_t db_max_bytes = ((metadata->wal_start-1) - metadata->db_start) * geo.lba_size;
		idx_t wal_max_bytes = ((metadata->tmp_start-1) - metadata->wal_start) * geo.lba_size;
		idx_t temp_max_bytes = ((geo.lba_count-1) - metadata->tmp_start) * geo.lba_size;

		idx_t db_used_bytes = (db_location.load() - metadata->db_start) * geo.lba_size;
		idx_t wal_used_bytes = (wal_location.load() - metadata->wal_start) * geo.lba_size;
		idx_t temp_used_bytes{};

		temp_lock.lock();
		for (const auto& kv : file_to_temp_meta) {
			temp_used_bytes += kv.second.block_size * kv.second.block_map.size();
		}
		temp_lock.unlock();

		remaining = (db_max_bytes - db_used_bytes) + (wal_max_bytes - wal_used_bytes) + (temp_max_bytes - temp_used_bytes);
	} else if (StringUtil::Equals(path.data(), NVMEFS_TMP_DIR_PATH.data())) {
		idx_t temp_max_bytes = ((geo.lba_count - 1) - metadata->tmp_start) * geo.lba_size;
		idx_t temp_used_bytes{};

		temp_lock.lock();
		for (const auto& kv : file_to_temp_meta) {
			temp_used_bytes += kv.second.block_size * kv.second.block_map.size();
		}
		temp_lock.unlock();
		remaining = (temp_max_bytes - temp_used_bytes);
	}
	return remaining;
}

Device &NvmeFileSystem::GetDevice() {
	return *device;
}

bool NvmeFileSystem::Trim(FileHandle &handle, idx_t offset_bytes, idx_t length_bytes) {
	data_ptr_t data = allocator.AllocateData(length_bytes);

	memset(data, 0, length_bytes);
	Write(handle, data, length_bytes, offset_bytes);

	allocator.FreeData(data, length_bytes);
	return true;
}

bool NvmeFileSystem::TryLoadMetadata() {
	if (metadata) {
		return true;
	}

	unique_ptr<GlobalMetadata> global = ReadMetadata();
	if (global) {
		metadata = std::move(global);
		return true;
	}

	return false;
}

void NvmeFileSystem::InitializeMetadata(const string &filename) {
	// We only support database paths/names up to 100 characters (this includes NVMEFS_PATH_PREFIX)
	if (filename.length() > 100) {
		throw IOException("Database name is too long.");
	}

	DeviceGeometry geo = device->GetDeviceGeometry();

	idx_t temp_start = (geo.lba_count - 1) - (max_temp_size / geo.lba_size);
	idx_t wal_lba_count = max_wal_size / geo.lba_size;
	idx_t wal_start = (temp_start - 1) - wal_lba_count;

	unique_ptr<GlobalMetadata> global = make_uniq<GlobalMetadata>(GlobalMetadata {});

	// 1 is the first LBA for the database because 0 is used for device metadata (global metadata)
	global->db_start = 1;
	global->wal_start = wal_start;
	global->tmp_start = temp_start;
	global->db_location = 0;
	global->wal_location = 0;
	global->db_path_size = filename.length();

	strncpy(global->db_path, filename.data(), filename.length());
	global->db_path[100] = '\0';

	temp_block_manager = make_uniq<NvmeTemporaryBlockManager>(temp_start, geo.lba_count - 1);

	WriteMetadata(*global);

	metadata = std::move(global);
}

unique_ptr<GlobalMetadata> NvmeFileSystem::ReadMetadata() {
	idx_t nr_bytes_magic = sizeof(NVMEFS_MAGIC_BYTES);
	idx_t nr_bytes_global = sizeof(GlobalMetadata);
	idx_t bytes_to_read = nr_bytes_magic + nr_bytes_global;

	data_ptr_t buffer = allocator.AllocateData(bytes_to_read);
	unique_ptr<GlobalMetadata> global = nullptr;

	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_READ;
	unique_ptr<FileHandle> fh = OpenFile(NVMEFS_GLOBAL_METADATA_PATH, flags);
	unique_ptr<CmdContext> cmd_ctx =
	    fh->Cast<NvmeFileHandle>().PrepareReadCommand(bytes_to_read, NVMEFS_GLOBAL_METADATA_LOCATION, 0);

	device->Read(buffer, *cmd_ctx);

	if (memcmp(buffer, NVMEFS_MAGIC_BYTES, nr_bytes_magic) == 0) {
		DeviceGeometry geo = device->GetDeviceGeometry();
		global = make_uniq<GlobalMetadata>(GlobalMetadata {});
		memcpy(global.get(), buffer + nr_bytes_magic, nr_bytes_global);
		temp_block_manager = make_uniq<NvmeTemporaryBlockManager>(global->tmp_start, geo.lba_count - 1);
	}

	allocator.FreeData(buffer, bytes_to_read);

	return std::move(global);
}

void NvmeFileSystem::WriteMetadata(GlobalMetadata &global) {
	idx_t nr_bytes_magic = sizeof(NVMEFS_MAGIC_BYTES);
	idx_t nr_bytes_global = sizeof(GlobalMetadata);
	idx_t bytes_to_write = nr_bytes_magic + nr_bytes_global;

	data_ptr_t buffer = allocator.AllocateData(bytes_to_write);
	memcpy(buffer, NVMEFS_MAGIC_BYTES, nr_bytes_magic);
	memcpy(buffer + nr_bytes_magic, &global, nr_bytes_global);

	FileOpenFlags flags = FileOpenFlags::FILE_FLAGS_WRITE;
	unique_ptr<FileHandle> fh = OpenFile(NVMEFS_GLOBAL_METADATA_PATH, flags);
	unique_ptr<CmdContext> cmd_ctx =
	    fh->Cast<NvmeFileHandle>().PrepareWriteCommand(bytes_to_write, NVMEFS_GLOBAL_METADATA_LOCATION, 0);

	device->Write(buffer, *cmd_ctx);

	allocator.FreeData(buffer, bytes_to_write);
}

void NvmeFileSystem::UpdateMetadata(CmdContext &context) {
	NvmeCmdContext &ctx = static_cast<NvmeCmdContext &>(context);
	MetadataType type = GetMetadataType(ctx.filepath);
	bool write = false;

	switch (type) {
	case MetadataType::WAL:
		if (ctx.start_lba >= wal_location.load()) {
			wal_location.store(ctx.start_lba + ctx.nr_lbas);
		}
		break;
	case MetadataType::TEMPORARY:
		// The temporary metadata remain static given that location is unused.
		// The file_to_temp_meta map will be updated during GetLBA, hence
		// no action is required here.
		break;
	case MetadataType::DATABASE:
		if (ctx.start_lba >= db_location.load()) {
			db_location.store(ctx.start_lba + ctx.nr_lbas);
		}
		break;
	default:
		throw InvalidInputException("no such metadatatype");
	}
}

MetadataType NvmeFileSystem::GetMetadataType(const string &filename) {
	if (StringUtil::Contains(filename, ".wal")) {
		return MetadataType::WAL;
	} else if (StringUtil::Contains(filename, "/tmp")) {
		return MetadataType::TEMPORARY;
	} else if (StringUtil::Contains(filename, ".db")) {
		return MetadataType::DATABASE;
	} else {
		throw InvalidInputException("Unknown file format");
	}
}

idx_t NvmeFileSystem::GetLBA(const string &filename, idx_t nr_bytes, idx_t location, idx_t nr_lbas) {
	idx_t lba {};
	MetadataType type = GetMetadataType(filename);
	DeviceGeometry geo = device->GetDeviceGeometry();

	idx_t lba_location = location / geo.lba_size;

	switch (type) {
	case MetadataType::WAL:
		if (lba_location < wal_location.load()) {
			lba = metadata->wal_start + lba_location;
		} else {
			lba = wal_location.load();
		}
		break;
	case MetadataType::TEMPORARY: {
		temp_lock.lock();
		TemporaryFileMetadata tfmeta;

		if (file_to_temp_meta.count(filename)) {
			tfmeta = file_to_temp_meta[filename];
			idx_t block_index = location / tfmeta.block_size;

			if (!tfmeta.block_map.count(block_index)) {
				TemporaryBlock *block = temp_block_manager->AllocateBlock(nr_lbas);
				tfmeta.block_map[block_index] = block;
			}
			lba = tfmeta.block_map[block_index]->GetStartLBA();
			file_to_temp_meta[filename] = tfmeta;

		} else {
			tfmeta = {.block_size = nr_lbas * geo.lba_size};
			file_to_temp_meta[filename] = tfmeta;

			idx_t block_index = location / tfmeta.block_size;

			TemporaryBlock *block = temp_block_manager->AllocateBlock(nr_lbas);
			file_to_temp_meta[filename].block_map[block_index] = block;
			lba = block->GetStartLBA();
		}
		temp_lock.unlock();
	} break;
	case MetadataType::DATABASE:
		lba = metadata->db_start + lba_location;
		break;
	default:
		throw InvalidInputException("No such metadata type");
		break;
	}

	return lba;
}

bool NvmeFileSystem::IsLBAInRange(const string &filename, idx_t start_lba, idx_t lba_count) {
	DeviceGeometry geo = device->GetDeviceGeometry();
	MetadataType type = GetMetadataType(filename);
	idx_t current_start{};
	idx_t current_end{};

	switch (type) {
	case MetadataType::WAL:
		current_start = metadata->wal_start;
		current_end = metadata->tmp_start - 1;
		break;
	case MetadataType::TEMPORARY:
		current_start = metadata->tmp_start;
		current_end = geo.lba_count - 1;
		break;
	case MetadataType::DATABASE:
		current_start = metadata->db_start;
		current_end = metadata->wal_start - 1;
		break;
	default:
		throw InvalidInputException("No such metadata type");
		break;
	}

	// Check if the LBA start location is within the range of the metadata range
	if ((start_lba < current_start || start_lba > current_end)) {
		return false;
	}

	// Check that if the lba is in range that we are not going to read or write out of range
	if ((start_lba + lba_count) > current_end) {
		return false;
	}

	return true;
}
} // namespace duckdb
