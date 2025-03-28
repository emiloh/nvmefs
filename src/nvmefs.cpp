#include "include/nvmefs.hpp"
#include "duckdb/common/string_util.hpp"
#include <libxnvme.h>

namespace duckdb {

/***************************
 * NvmeFileHandle
 ****************************/

NvmeFileHandle::NvmeFileHandle(FileSystem &file_system, string path, uint8_t plid_idx, xnvme_dev *device,
                               uint8_t plid_count, FileOpenFlags flags, bool internal_fileHandle)
    : FileHandle(file_system, path, flags) {
	// Get placemenet handle indentifier and create placement idenetifier
	// Inspiration: https://github.com/xnvme/xnvme/blob/be52a634c139647b14940ba8a3ff254d6b1ca8c4/tools/xnvme.c#L833
	this->cursor_offset = 0;
	this->device = device;
	this->internal_fileHandle = internal_fileHandle;

	struct xnvme_cmd_ctx ctx = xnvme_cmd_ctx_from_dev(device);
	uint32_t nsid = xnvme_dev_get_nsid(device);

	struct xnvme_spec_ruhs *ruhs = nullptr;
	uint32_t ruhs_nbytes = sizeof(*ruhs) + plid_count + sizeof(struct xnvme_spec_ruhs_desc);

	ruhs = (struct xnvme_spec_ruhs *)xnvme_buf_alloc(device, ruhs_nbytes);
	memset(ruhs, 0, ruhs_nbytes);

	xnvme_nvm_mgmt_recv(&ctx, nsid, XNVME_SPEC_IO_MGMT_RECV_RUHS, 0, ruhs, ruhs_nbytes);

	uint16_t phid = ruhs->desc[plid_idx].pi;

	this->placement_identifier_count = plid_count;
	this->placement_identifier = phid << 16;

	xnvme_buf_free(device, ruhs);
}

NvmeFileHandle::~NvmeFileHandle() {
	if (!internal_fileHandle) {
		xnvme_dev_close(device);
	}
};

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

void NvmeFileHandle::SetFilePointer(idx_t location) {
	cursor_offset = location;
}

idx_t NvmeFileHandle::GetFilePointer() {
	return cursor_offset;
}

/// @brief Calculates the amount of LBAs required to store the given number of bytes
/// @param lba_size The size of a single LBA
/// @param nr_bytes The number of bytes to store
/// @return The number of LBAs required to store the given number of bytes
int64_t CalculateRequiredLBACount(uint32_t lba_size, uint64_t nr_bytes) {

	return (nr_bytes + lba_size - 1) / lba_size; // Round up to nearest LBA
}

unique_ptr<NvmeCmdContext> NvmeFileHandle::PrepareWriteCommand(int64_t nr_bytes) {

	D_ASSERT(nr_bytes > 0);

	xnvme_cmd_ctx ctx = xnvme_cmd_ctx_from_dev(device);
	uint32_t nsid = xnvme_dev_get_nsid(device);

	ctx.cmd.common.cdw13 = placement_identifier;

	const xnvme_geo *geo = xnvme_dev_get_geo(device);

	unique_ptr<NvmeCmdContext> nvme_ctx = make_uniq<NvmeCmdContext>();
	nvme_ctx->ctx = ctx;
	nvme_ctx->namespace_id = nsid;
	nvme_ctx->number_of_lbas = CalculateRequiredLBACount(geo->lba_nbytes, nr_bytes);

	return std::move(nvme_ctx);
}

unique_ptr<NvmeCmdContext> NvmeFileHandle::PrepareReadCommand(int64_t nr_bytes) {

	D_ASSERT(nr_bytes > 0);

	xnvme_cmd_ctx ctx = xnvme_cmd_ctx_from_dev(device);
	uint32_t nsid = xnvme_dev_get_nsid(device);

	ctx.cmd.common.cdw13 = placement_identifier;

	const xnvme_geo *geo = xnvme_dev_get_geo(device);

	unique_ptr<NvmeCmdContext> nvme_ctx = make_uniq<NvmeCmdContext>();
	nvme_ctx->ctx = ctx;
	nvme_ctx->namespace_id = nsid;
	nvme_ctx->number_of_lbas = CalculateRequiredLBACount(geo->lba_nbytes, nr_bytes);

	return std::move(nvme_ctx);
}

nvme_buf_ptr NvmeFileHandle::AllocateDeviceBuffer(int64_t nr_bytes) {
	return xnvme_buf_alloc(device, nr_bytes);
}

void NvmeFileHandle::FreeDeviceBuffer(nvme_buf_ptr buffer) {
	xnvme_buf_free(device, buffer);
}

/***************************
 * NvmeFileSystem
 ****************************/

NvmeFileSystem::NvmeFileSystem(NvmeFileSystemProxy &proxy_ref) : proxy_filesystem(proxy_ref) {
	allocated_paths.push_back("nvmefs:///tmp");
	allocated_placement_identifiers["nvmefs:///tmp"] = 1;
}

NvmeFileSystem::NvmeFileSystem(NvmeFileSystemProxy &proxy_ref, const string &dev_path, const uint64_t placement_handls)
    : proxy_filesystem(proxy_ref), device_path(dev_path), plhdls(placement_handls) {
	allocated_paths.push_back("nvmefs:///tmp");
	allocated_placement_identifiers["nvmefs:///tmp"] = 1;
}

unique_ptr<FileHandle> NvmeFileSystem::OpenFile(const string &path, FileOpenFlags flags,
                                                optional_ptr<FileOpener> opener) {
	// Create NvmeFileHandler
	auto xnvme_opts = xnvme_opts_default();
	xnvme_dev *device = xnvme_dev_open(device_path.c_str(), &xnvme_opts);

	// If device is not opened then we should fail... for now return null
	if (!device) {
		xnvme_cli_perr("xnvme_dev_open()", errno);
		return nullptr;
	}

	// Get and add placement identifier for path
	uint8_t placement_identifier_index = GetPlacementIdentifierIndexOrDefault(path);

	unique_ptr<NvmeFileHandle> file_handler =
	    make_uniq<NvmeFileHandle>(proxy_filesystem, path, placement_identifier_index, device, plhdls, flags, false);

	return std::move(file_handler);
}

unique_ptr<MetadataFileHandle> NvmeFileSystem::OpenMetadataFile(FileHandle &handle, string path, FileOpenFlags flags) {

	// Get FDP placement identifier specifically for metadata
	uint8_t placement_identifier_index = GetPlacementIdentifierIndexOrDefault(path);

	// Cast handle to NvmeFileHandle to get access to specical context
	NvmeFileHandle &fh = handle.Cast<NvmeFileHandle>();
	xnvme_dev *device = fh.device;
	uint8_t plid_count = fh.placement_identifier_count;

	unique_ptr<NvmeFileHandle> file_handler =
	    make_uniq<NvmeFileHandle>(proxy_filesystem, path, placement_identifier_index, device, plid_count, flags, true);

	return std::move(file_handler);
}

/*
 * Get placement identifier for path. If path is not allocated
 * to a specific placement identifier index then return default placement identifier
 */
uint8_t NvmeFileSystem::GetPlacementIdentifierIndexOrDefault(const string &path) {
	int8_t placement_identifier_index = 0; // Set to default index for now
	for (auto &path_prefix : allocated_paths) {
		// Check if path starts with path_prefix
		if (StringUtil::StartsWith(path, path_prefix)) {
			// Get placement identifier index
			placement_identifier_index = allocated_placement_identifiers[path_prefix];
			break;
		}
	}

	return placement_identifier_index;
}

void NvmeFileSystem::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	ReadInternal(handle, buffer, nr_bytes, location, 0);
}

void NvmeFileSystem::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	// fall-back - throw away number of LBA's written
	WriteInternal(handle, buffer, nr_bytes, location, 0);
}

uint64_t NvmeFileSystem::WriteInternal(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location_lba,
                                       idx_t in_block_offset) {
	NvmeFileHandle &nvme_handle = handle.Cast<NvmeFileHandle>();

	unique_ptr<NvmeCmdContext> nvme_ctx = nvme_handle.PrepareWriteCommand(nr_bytes);
	D_ASSERT(nvme_ctx->number_of_lbas > 0);
	D_ASSERT((in_block_offset == 0 && nvme_ctx->number_of_lbas > 1) ||
	         (in_block_offset >= 0 &&
	          nvme_ctx->number_of_lbas == 1)); // We only support in-block writing of a single block... for now

	nvme_buf_ptr dev_buffer = nvme_handle.AllocateDeviceBuffer(nr_bytes);
	if (in_block_offset > 0) {
		D_ASSERT(in_block_offset + nr_bytes < NVME_BLOCK_SIZE);           // Be sure that the write fits within a block
		Read(handle, dev_buffer, nvme_ctx->number_of_lbas, location_lba); // Reads the whole block
	}

	memcpy(dev_buffer, buffer + in_block_offset, nr_bytes);

	int err = xnvme_nvm_write(&nvme_ctx->ctx, nvme_ctx->namespace_id, location_lba, nvme_ctx->number_of_lbas - 1,
	                          dev_buffer, nullptr);
	if (err) {
		// TODO: Handle error
		xnvme_cli_perr("xnvme_nvm_write()", err);
		throw IOException("Error writing to NVMe device");
	}

	nvme_handle.FreeDeviceBuffer(dev_buffer);

	return nvme_ctx->number_of_lbas;
}

uint64_t NvmeFileSystem::ReadInternal(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location_lba,
                                      idx_t in_block_offset) {

	NvmeFileHandle &nvme_handle = handle.Cast<NvmeFileHandle>();

	unique_ptr<NvmeCmdContext> nvme_ctx = nvme_handle.PrepareReadCommand(nr_bytes);
	D_ASSERT(nvme_ctx->number_of_lbas > 0);

	nvme_buf_ptr dev_buffer = nvme_handle.AllocateDeviceBuffer(nr_bytes);

	int err = xnvme_nvm_read(&nvme_ctx->ctx, nvme_ctx->namespace_id, location_lba, nvme_ctx->number_of_lbas - 1,
	                         dev_buffer, nullptr);
	if (err) {
		// TODO: Handle error
		throw IOException("Error reading from NVMe device");
	}

	// Copy the data from the device buffer to the output buffer. We apply the offset, to read from the specific
	// location within the block and only return that particular data
	memcpy(buffer, dev_buffer + in_block_offset, nr_bytes);
	nvme_handle.FreeDeviceBuffer(dev_buffer);

	return nvme_ctx->number_of_lbas;
}

bool NvmeFileSystem::CanHandleFile(const string &path) {
	return StringUtil::StartsWith(path, NVMEFS_PATH_PREFIX);
}

void NvmeFileSystem::Seek(FileHandle &handle, idx_t location) {
	handle.Cast<NvmeFileHandle>().SetFilePointer(location);
}

idx_t NvmeFileSystem::SeekPosition(FileHandle &handle) {
	return handle.Cast<NvmeFileHandle>().GetFilePointer();
}

unique_ptr<NvmeDeviceGeometry> NvmeFileSystem::GetDeviceGeometry() {

	xnvme_dev *device = xnvme_dev_open(device_path.c_str(), nullptr);

	unique_ptr<NvmeDeviceGeometry> device_geo = make_uniq<NvmeDeviceGeometry>(NvmeDeviceGeometry {});
	const xnvme_geo *geo = xnvme_dev_get_geo(device);
	const xnvme_spec_idfy_ns *nsgeo = xnvme_dev_get_ns(device);

	device_geo->lba_size = geo->lba_nbytes;
	device_geo->lba_count = nsgeo->nsze;

	xnvme_dev_close(device);

	return move(device_geo);
}
} // namespace duckdb
