#pragma once

#include "duckdb.hpp"
#include <future>

namespace duckdb {

enum RequestType { READ, WRITE };

typedef void *backend_buf_ptr;

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

	backend_buf_ptr GetBuffer();
	idx_t GetBufferSizeInBytes();
	idx_t GetLBALocation();
	idx_t GetLBACount();
	uint8_t GetFDPPlacementIndex();
	bool IsRead();
	bool IsWrite();

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
	bool WaitForCompletion() override;
};

class AsyncIORequest : public IORequest {
public:
	AsyncIORequest(RequestOptions opts, RequestType type)
	    : IORequest(opts, type), promise(), future(promise.get_future()) {
		// Constructor implementation
	}

	void Failed();

	void Success();

	/// @brief Waits for the request to complete. This is a blocking call.
	/// @return true if the request completed successfully, false otherwise
	bool WaitForCompletion() override;

private:
	std::promise<bool> promise;
	std::future<bool> future;
};

} // namespace duckdb
