#include "duckdb.hpp"
#include "xnvme_device.hpp"
#include "nvme_device.hpp"

namespace duckdb {

XNVMeDevice::XNVMeDevice(const string &device_path, const idx_t placement_handles, const string &backend,
                         const bool async) {
	// Initialize the device
	if (async) {
		this->backend = make_uniq<AsyncIOBackend>(device_path, placement_handles, backend);
	} else {
		this->backend = make_uniq<SyncIOBackend>(device_path, placement_handles, backend);
	}

	allocated_placement_identifiers["nvmefs:///tmp"] = 1;
}

XNVMeDevice::~XNVMeDevice() {
}

DeviceGeometry XNVMeDevice::GetDeviceGeometry() {
	DeviceGeometry geometry;

	geometry.lba_size = backend->GetLBASize();
	geometry.lba_count = backend->GetLBACount();

	return geometry;
}

static uint8_t GetPlacementIdentifierOrDefault(map<string, uint8_t> identifiers, const string &path) {
	uint8_t placement_identifier = 0;
	for (const auto &kv : identifiers) {
		if (StringUtil::StartsWith(path, kv.first)) {
			placement_identifier = kv.second;
		}
	}

	return placement_identifier;
}

idx_t XNVMeDevice::Read(void *buffer, const CmdContext &context) {
	D_ASSERT(context.nr_lbas > 0);
	// We only support offset reads within a single block
	D_ASSERT((context.offset == 0 && context.nr_lbas > 1) || (context.offset >= 0 && context.nr_lbas == 1));

	string filepath = static_cast<const NvmeCmdContext &>(context).filepath;
	uint8_t plid_idx = GetPlacementIdentifierOrDefault(allocated_placement_identifiers, filepath);

	backend_buf_ptr backend_buffer = backend->AllocateBuffer(context.nr_bytes);
	unique_ptr<IORequest> req =
	    backend->CreateReadRequest(context.start_lba, context.nr_lbas, plid_idx, backend_buffer);

	if (!backend->SubmitRequest(req.get())) {
		backend->FreeBuffer(backend_buffer);
		throw InternalException("Failed to submit read request");
	}

	if (!req->WaitForCompletion()) {
		backend->FreeBuffer(backend_buffer);
		throw InternalException("Failed to wait for read request completion");
	}

	memcpy(buffer, (char *)backend_buffer + context.offset, context.nr_bytes);

	backend->FreeBuffer(backend_buffer);

	return context.nr_lbas;
}

idx_t XNVMeDevice::Write(void *buffer, const CmdContext &context) {

	// Handle in block writes
	backend_buf_ptr backend_buffer = backend->AllocateBuffer(context.nr_bytes);
	if (context.offset > 0) {
		// Check if write is fully contained within single block
		D_ASSERT(context.offset + context.nr_bytes < backend->GetLBASize());
		// Read the whole LBA block
		Read(backend_buffer, context);
	}
	memcpy(backend_buffer, (char *)buffer + context.offset, context.nr_bytes);

	string filepath = static_cast<const NvmeCmdContext &>(context).filepath;
	uint8_t plid_idx = GetPlacementIdentifierOrDefault(allocated_placement_identifiers, filepath);

	unique_ptr<IORequest> req =
	    backend->CreateWriteRequest(context.start_lba, context.nr_lbas, plid_idx, backend_buffer);

	if (!backend->SubmitRequest(req.get())) {
		backend->FreeBuffer(backend_buffer);
		throw InternalException("Failed to submit read request");
	}

	if (!req->WaitForCompletion()) {
		backend->FreeBuffer(backend_buffer);
		throw InternalException("Failed to wait for read request completion");
	}

	backend->FreeBuffer(backend_buffer);

	return context.nr_lbas;
}

} // namespace duckdb
