#include "io_backend.hpp"

namespace duckdb {

IOBackend::~IOBackend() {
	xnvme_dev_close(device);
}

idx_t IOBackend::SubmitRequest(IORequest *request) {
	throw NotImplementedException("%s: SubmitRequest is not implemented", GetName());
}

unique_ptr<IORequest> IOBackend::CreateReadRequest(idx_t lba_location, idx_t nr_lbas, uint8_t plid,
                                                   backend_buf_ptr buffer) {
	throw NotImplementedException("%s: SubmitRequest is not implemented", GetName());
}

unique_ptr<IORequest> IOBackend::CreateWriteRequest(idx_t lba_location, idx_t nr_lbas, uint8_t plid,
                                                    backend_buf_ptr buffer) {
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

unique_ptr<IORequest> SyncIOBackend::CreateReadRequest(idx_t lba_location, idx_t nr_lbas, uint8_t plid,
                                                       backend_buf_ptr buffer) {
	RequestOptions opts;
	opts.fdp_placement_index = plid;
	opts.lba_location = lba_location;
	opts.lba_size = geometry.lba_size;
	opts.lba_count = nr_lbas;
	opts.buffer = buffer;

	return make_uniq<SyncIORequest>(opts, RequestType::READ);
}

unique_ptr<IORequest> SyncIOBackend::CreateWriteRequest(idx_t lba_location, idx_t nr_lbas, uint8_t plid,
                                                        backend_buf_ptr buffer) {

	RequestOptions opts;
	opts.fdp_placement_index = plid;
	opts.lba_location = lba_location;
	opts.lba_size = geometry.lba_size;
	opts.lba_count = nr_lbas;
	opts.buffer = buffer;

	return make_uniq<SyncIORequest>(opts, RequestType::WRITE);
}

idx_t SyncIOBackend::SubmitRequest(IORequest *request) {
	xnvme_cmd_ctx ctx = xnvme_cmd_ctx_from_dev(device);

	idx_t lba_location = request->GetLBALocation();
	idx_t lba_count = request->GetLBACount();
	backend_buf_ptr fs_buffer = request->GetBuffer();

	if (request->IsRead()) {
		int err = xnvme_nvm_read(&ctx, geometry.device_ns_id, lba_location, lba_count - 1, fs_buffer, nullptr);
		if (err) {
			xnvme_cli_perr("Could not read from device with xnvme_nvme_read(): ", err);
			return 1;
		}
	} else {

		int err = xnvme_nvm_write(&ctx, geometry.device_ns_id, lba_location, lba_count - 1, fs_buffer, nullptr);
		if (err) {
			xnvme_cli_perr("Could not write to device with xnvme_nvme_write(): ", err);
			return 1;
		}
	}

	return 0;
}

void SyncIOBackend::Sync() {
}

/**********************
 * AsyncIOBackend
 *********************/

AsyncIOBackend::AsyncIOBackend(const string &device_path, const idx_t placement_handles, const string &backend)
    : IOBackend(device_path, placement_handles, backend, true) {

	int err = xnvme_queue_init(device, 16, 0, &queue); // Queue depth of 16... for now
	if (err) {
		xnvme_cli_perr("Unable to create an queue for asynchronous IO", err);
	}

	stop_event_loop.store(false);
	sync_io_requests.store(false);
	RunEventLoop();
}

AsyncIOBackend::~AsyncIOBackend() {
	StopEventLoop();
	// TODO: Stop the event loop and cleanup the queue
	xnvme_queue_term(queue);
}

unique_ptr<IORequest> AsyncIOBackend::CreateReadRequest(idx_t lba_location, idx_t nr_lbas, uint8_t plid,
                                                        backend_buf_ptr buffer) {

	RequestOptions opts;
	opts.fdp_placement_index = plid;
	opts.lba_location = lba_location;
	opts.lba_size = geometry.lba_size;
	opts.lba_count = nr_lbas;
	opts.buffer = buffer;

	return make_uniq<AsyncIORequest>(opts, RequestType::READ);
}

unique_ptr<IORequest> AsyncIOBackend::CreateWriteRequest(idx_t lba_location, idx_t nr_lbas, uint8_t plid,
                                                         backend_buf_ptr buffer) {

	RequestOptions opts;
	opts.fdp_placement_index = plid;
	opts.lba_location = lba_location;
	opts.lba_size = geometry.lba_size;
	opts.lba_count = nr_lbas;
	opts.buffer = buffer;

	return make_uniq<AsyncIORequest>(opts, RequestType::WRITE);
}

idx_t AsyncIOBackend::SubmitRequest(IORequest *request) {
	AsyncIORequest *async_request = static_cast<AsyncIORequest *>(request);
	request_queue.enqueue(async_request);

	return 0;
}

void AsyncIOBackend::Sync() {
	// NOTE: This is probably a naive approach.. but i am tired
	sync_io_requests.store(true);
}

void AsyncIOBackend::RunEventLoop() {
	auto loop_function = [this]() {
		while (!stop_event_loop.load()) {

			if (sync_io_requests.load()) {
				size_t items_in_queue = request_queue.size_approx();

				// Process requests from the queue
				ProcessRequestFromQueue(items_in_queue);

				xnvme_queue_drain(queue); // Process all completed items
				sync_io_requests.store(false);
				continue;
			}

			// Process requests from the queue
			ProcessRequestFromQueue(8);

			// Wait for an event to be available
			xnvme_queue_poke(queue, 0);
		}
	};

	event_loop_thread = std::thread(loop_function); // Start the event loop in a separate thread
}

void AsyncIOBackend::StopEventLoop() {

	stop_event_loop.store(true);
	event_loop_thread.join();
}

void AsyncIOBackend::ProcessRequestFromQueue(idx_t max_items) {
	// Process requests from the queue
	AsyncIORequest *requests[8];
	size_t nr_dequeued_items = request_queue.try_dequeue_bulk(requests, max_items);

	for (size_t i = 0; i < nr_dequeued_items; i++) {
		AsyncIORequest *request = requests[i];
		xnvme_cmd_ctx *xnvme_ctx = xnvme_queue_get_cmd_ctx(queue);
		PrepareRequest(xnvme_ctx, *request);

		idx_t lba_location = request->GetLBALocation();
		idx_t lba_count = request->GetLBACount();

		// Set the command context
		xnvme_cmd_ctx_set_cb(xnvme_ctx, RequestCallback, request);

		// Prepare the command
		int err = 0; // If successful, ret will be 0
		int retries = 3;
		do {
			if (request->IsRead()) {
				err = xnvme_nvm_read(xnvme_ctx, geometry.device_ns_id, lba_location, lba_count - 1,
				                     request->GetBuffer(), nullptr);
			} else {
				err = xnvme_nvm_write(xnvme_ctx, geometry.device_ns_id, lba_location, lba_count - 1,
				                      request->GetBuffer(), nullptr);
			}

			if (err == -EBUSY)
				xnvme_queue_poke(queue, 0); // Submission queue is full. Go through all completed items

			retries--;
		} while (err && retries > 0);

		if (err && retries <= 0) {
			xnvme_cli_perr("Encountered error when submitting request to NVMe device: ", err);
			request->Failed();
			xnvme_queue_put_cmd_ctx(queue, xnvme_ctx);
		}
	}
}

void AsyncIOBackend::PrepareRequest(xnvme_cmd_ctx *ctx, AsyncIORequest &request) {
	// Retrieve information about recliam unit handles
	struct xnvme_spec_ruhs *ruhs = nullptr;
	// TODO: verify this calculation!!
	uint32_t ruhs_nbytes = sizeof(*ruhs) + geometry.plhdls + sizeof(struct xnvme_spec_ruhs_desc);
	ruhs = (struct xnvme_spec_ruhs *)xnvme_buf_alloc(device, ruhs_nbytes);
	memset(ruhs, 0, ruhs_nbytes);
	xnvme_nvm_mgmt_recv(ctx, geometry.device_ns_id, XNVME_SPEC_IO_MGMT_RECV_RUHS, 0, ruhs, ruhs_nbytes);

	uint16_t phid = ruhs->desc[request.GetFDPPlacementIndex()].pi;
	ctx->cmd.common.cdw13 = phid << 16;

	xnvme_buf_free(device, ruhs);
}

void AsyncIOBackend::RequestCallback(xnvme_cmd_ctx *ctx, void *data) {
	// Handle the completion of the request
	if (!data)
		return;

	AsyncIORequest *request = static_cast<AsyncIORequest *>(data);
	printf("Request completed: %p\n", request);

	if (xnvme_cmd_ctx_cpl_status(ctx)) {
		xnvme_queue_put_cmd_ctx(ctx->async.queue, ctx);
		ctx->async.cb_arg = nullptr;
		request->Failed();
		return;
	}

	xnvme_queue_put_cmd_ctx(ctx->async.queue, ctx);
	ctx->async.cb_arg = nullptr;
	request->Success();
}

} // namespace duckdb
