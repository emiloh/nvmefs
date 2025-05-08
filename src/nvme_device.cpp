#include "nvme_device.hpp"

namespace duckdb {

NvmeDevice::NvmeDevice(const string &device_path, const idx_t placement_handles, const string &backend,
                       const bool async)
    : dev_path(device_path), plhdls(placement_handles) {

	if (async) {
		this->backend = make_uniq<AsyncIOBackend>(device_path, placement_handles, backend);
	} else {
		this->backend = make_uniq<SyncIOBackend>(device_path, placement_handles, backend);
	}

	allocated_placement_identifiers["nvmefs:///tmp"] = 1;
	geometry = LoadDeviceGeometry();
}

NvmeDevice::~NvmeDevice() {
}

idx_t NvmeDevice::Write(void *buffer, const CmdContext &context) {

	const NvmeCmdContext &ctx = static_cast<const NvmeCmdContext &>(context);
	D_ASSERT(ctx.nr_lbas > 0);
	// We only support offset writes within a single block
	D_ASSERT((ctx.offset == 0 && ctx.nr_lbas > 1) || (ctx.offset >= 0 && ctx.nr_lbas == 1));

	backend_buf_ptr dev_buffer = backend->AllocateBuffer(ctx.nr_bytes);
	if (ctx.offset > 0) {
		// Check if write is fully contained within single block
		D_ASSERT(ctx.offset + ctx.nr_bytes < geometry.lba_size);
		// Read the whole LBA block
		Read(dev_buffer, ctx);
	}

	memcpy(dev_buffer, (char *)buffer + ctx.offset, ctx.nr_bytes);
	uint8_t plid_idx = GetPlacementIdentifierOrDefault(ctx.filepath);

	shared_ptr<IORequest> req = backend->CreateWriteRequest(ctx.start_lba, ctx.nr_lbas, plid_idx, dev_buffer);
	int err = backend->SubmitRequest(req);
	if (err) {
		backend->FreeBuffer(dev_buffer);
		throw InternalException("Failed to submit write request");
	}

	if (!req->WaitForCompletion()) {
		backend->FreeBuffer(dev_buffer);
		throw InternalException("Failed to wait for write request completion");
	}

	backend->FreeBuffer(dev_buffer);

	return ctx.nr_lbas;
}

idx_t NvmeDevice::Read(void *buffer, const CmdContext &context) {
	const NvmeCmdContext &ctx = static_cast<const NvmeCmdContext &>(context);

	D_ASSERT(ctx.nr_lbas > 0);
	// We only support offset reads within a single block
	D_ASSERT((ctx.offset == 0 && ctx.nr_lbas > 1) || (ctx.offset >= 0 && ctx.nr_lbas == 1));

	backend_buf_ptr dev_buffer = backend->AllocateBuffer(ctx.nr_bytes);
	uint8_t plid_idx = GetPlacementIdentifierOrDefault(ctx.filepath);

	shared_ptr<IORequest> req = backend->CreateReadRequest(ctx.start_lba, ctx.nr_lbas, plid_idx, dev_buffer);

	int err = backend->SubmitRequest(req);
	if (err) {
		backend->FreeBuffer(dev_buffer);
		throw InternalException("Failed to submit read request");
	}

	if (!req->WaitForCompletion()) {
		backend->FreeBuffer(dev_buffer);
		throw InternalException("Failed to wait for read request completion");
	}

	memcpy(buffer, (char *)dev_buffer + ctx.offset, ctx.nr_bytes);

	backend->FreeBuffer(dev_buffer);

	return ctx.nr_lbas;
}

DeviceGeometry NvmeDevice::GetDeviceGeometry() {
	return geometry;
}

uint8_t NvmeDevice::GetPlacementIdentifierOrDefault(const string &path) {
	uint8_t placement_identifier = 0;
	for (const auto &kv : allocated_placement_identifiers) {
		if (StringUtil::StartsWith(path, kv.first)) {
			placement_identifier = kv.second;
		}
	}

	return placement_identifier;
}

DeviceGeometry NvmeDevice::LoadDeviceGeometry() {
	NvmeDeviceGeometry geometry {};

	geometry.lba_size = backend->GetLBASize();
	geometry.lba_count = backend->GetLBACount();

	return geometry;
}

} // namespace duckdb
