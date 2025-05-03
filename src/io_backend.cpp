#include "io_backend.hpp"

namespace duckdb {

IOBackend::~IOBackend() {
	xnvme_dev_close(device);
}

idx_t IOBackend::SubmitRequest(IORequest request) {
	throw NotImplementedException("%s: SubmitRequest is not implemented", GetName());
}

IORequest IOBackend::CreateReadRequest(idx_t lba_location, idx_t nr_lbas, backend_buf_ptr buffer) {
	throw NotImplementedException("%s: SubmitRequest is not implemented", GetName());
}

IORequest IOBackend::CreateWriteRequest(idx_t lba_location, idx_t nr_lbas, backend_buf_ptr buffer) {
	throw NotImplementedException("%s: SubmitRequest is not implemented", GetName());
}

void IOBackend::Sync() {
	throw NotImplementedException("%s: Sync is not implemented", GetName());
}

/**********************
 * SyncIOBackend
 *********************/

SyncIOBackend::SyncIOBackend(const string &device_path, const idx_t placement_handles, const string &backend)
    : IOBackend(device_path, placement_handles, backend, false) {
	// Initialize the device
}

SyncIOBackend::~SyncIOBackend() {
	// Cleanup resources
}

IORequest SyncIOBackend::CreateReadRequest(idx_t lba_location, idx_t nr_lbas, backend_buf_ptr buffer) {

	return SyncIORequest(lba_location, geometry.lba_size, nr_lbas, buffer, RequestType::READ);
}

IORequest SyncIOBackend::CreateWriteRequest(idx_t lba_location, idx_t nr_lbas, backend_buf_ptr buffer) {

	return SyncIORequest(lba_location, geometry.lba_size, nr_lbas, buffer, RequestType::WRITE);
}

idx_t SyncIOBackend::SubmitRequest(IORequest request) {
	xnvme_cmd_ctx ctx = xnvme_cmd_ctx_from_dev(device);

	idx_t lba_location = request.GetLBALocation();
	idx_t lba_count = request.GetLBACount();
	backend_buf_ptr fs_buffer = request.GetBuffer();

	if (request.IsRead()) {
		int err = xnvme_nvm_read(&ctx, geometry.device_ns_id, lba_location, lba_count - 1, fs_buffer, nullptr);
		if (err) {
			xnvme_cli_perr("Could not read from device with xnvme_nvme_read(): ", err);
			throw IOException("Encountered error when reading from NVMe device");
		}
	} else {

		int err = xnvme_nvm_write(&ctx, geometry.device_ns_id, lba_location, lba_count - 1, fs_buffer, nullptr);
		if (err) {
			xnvme_cli_perr("Could not write to device with xnvme_nvme_write(): ", err);
			throw IOException("Encountered error when writing to NVMe device");
		}
	}
}

/**********************
 * AsyncIOBackend
 *********************/

AsyncIOBackend::AsyncIOBackend(const string &device_path, const idx_t placement_handles, const string &backend)
    : IOBackend(device_path, placement_handles, backend, true) {
	// TOOD: Init the queue and start the event loop
}

AsyncIOBackend::~AsyncIOBackend() {
	// TODO: Stop the event loop and cleanup the queue
}

IORequest AsyncIOBackend::CreateReadRequest(idx_t lba_location, idx_t nr_lbas, backend_buf_ptr buffer) {
	return AsyncIORequest(lba_location, geometry.lba_size, nr_lbas, buffer, RequestType::READ);
}

IORequest AsyncIOBackend::CreateWriteRequest(idx_t lba_location, idx_t nr_lbas, backend_buf_ptr buffer) {
	return AsyncIORequest(lba_location, geometry.lba_size, nr_lbas, buffer, RequestType::WRITE);
}

idx_t AsyncIOBackend::SubmitRequest(IORequest request) {
	request_queue.enqueue(request);

	return 0;
}

} // namespace duckdb
