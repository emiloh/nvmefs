#pragma once

#include "duckdb.hpp"
#include <future>

namespace duckdb {

enum RequestType { READ, WRITE };

struct RequestOptions {

	// Offset in a block of an lba to read/write
	uint8_t fdp_placement_index;
	idx_t lba_location;
	idx_t lba_size;
	idx_t lba_count;
	backend_buf_ptr buffer;
};

class IORequest {
public:
	IORequest(RequestOptions opts, RequestType type) : options(opts), type(type) {
	}

	virtual bool WaitForCompletion();

	backend_buf_ptr GetBuffer() {
		return options.buffer;
	}

	idx_t GetBufferSizeInBytes() {
		return options.lba_count * options.lba_size;
	}

	idx_t GetLBALocation() {
		return options.lba_location;
	}

	idx_t GetLBACount() {
		return options.lba_count;
	}

	uint8_t GetFDPPlacementIndex() {
		return options.fdp_placement_index;
	}

	bool IsRead() {
		return type == RequestType::READ;
	}

	bool IsWrite() {
		return type == RequestType::WRITE;
	}

private:
	RequestOptions options;
	RequestType type;
};

class SyncIORequest : public IORequest {
public:
	SyncIORequest(RequestOptions opts, RequestType type) : IORequest(opts, type) {
		// Constructor implementation
	}

	/// @brief Just returns true. Assumes that if an error has happend it was at the submission of the request.
	/// @return True
	bool WaitForCompletion() override {
		// Implementation for synchronous request completion
		return true;
	}
};

class AsyncIORequest : public IORequest {
public:
	AsyncIORequest(RequestOptions opts, RequestType type)
	    : IORequest(opts, type), promise(make_uniq<std::promise<bool>>()), future(promise->get_future()) {
		// Constructor implementation
	}

	void Failed() {
		// Set the promise to indicate failure
		promise->set_value(false);
	}

	void Success() {
		// Set the promise to indicate success
		promise->set_value(true);
	}

	/// @brief Waits for the request to complete. This is a blocking call.
	/// @return true if the request completed successfully, false otherwise
	bool WaitForCompletion() override {
		// Implementation for asynchronous request completion
		return future.get();
	}

private:
	unique_ptr<std::promise<bool>> promise;
	std::future<bool> future;
};

} // namespace duckdb
