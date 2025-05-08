#include "io_request.hpp"

namespace duckdb {

backend_buf_ptr IORequest::GetBuffer() {
	std::lock_guard<std::mutex> lock(buffer_mutex);
	return options.buffer;
}

idx_t IORequest::GetBufferSizeInBytes() {
	return options.lba_count * options.lba_size;
}

idx_t IORequest::GetLBALocation() {
	return options.lba_location;
}

idx_t IORequest::GetLBACount() {
	return options.lba_count;
}

uint8_t IORequest::GetFDPPlacementIndex() {
	return options.fdp_placement_index;
}

bool IORequest::IsRead() {
	return type == RequestType::READ;
}

bool IORequest::IsWrite() {
	return type == RequestType::WRITE;
}

bool IORequest::WaitForCompletion() {
	throw NotImplementedException("WaitForCompletion is not implemented");
}

bool SyncIORequest::WaitForCompletion() {
	// Implementation for synchronous request completion
	return true;
}

void AsyncIORequest::Failed() {
	promise.set_value(false);
}

void AsyncIORequest::Success() {
	promise.set_value(true);
}

bool AsyncIORequest::WaitForCompletion() {
	return future.get();
}
} // namespace duckdb
