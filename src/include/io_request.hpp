#pragma once

#include "duckdb.hpp"
#include <future>

namespace duckdb {

enum RequestType { READ, WRITE };

struct FileSystemBuffer {
	// Buffer from the file system
	void *buffer;

	// Size of the buffer
	idx_t size;

	// Offset in a block of an lba to read/write
	idx_t offset;
};

class IORequest {
public:
	IORequest(idx_t lba_location, idx_t lba_size, idx_t nr_lbas, backend_buf_ptr buffer, RequestType type)
	    : lba_location(lba_location), lba_count(nr_lbas), buffer(buffer), type(type), lba_size(lba_size) {
	}

	virtual bool WaitForCompletion();

	backend_buf_ptr GetBuffer() {
		return buffer;
	}

	idx_t GetBufferSizeInBytes() {
		return lba_count * lba_size;
	}

	idx_t GetLBALocation() {
		return lba_location;
	}

	idx_t GetLBACount() {
		return lba_count;
	}

	bool IsRead() {
		return type == RequestType::READ;
	}

	bool IsWrite() {
		return type == RequestType::WRITE;
	}

private:
	idx_t lba_location;
	idx_t lba_count;
	idx_t lba_size;
	RequestType type;
	backend_buf_ptr buffer;
};

class SyncIORequest : public IORequest {
public:
	SyncIORequest(idx_t lba_location, idx_t lba_size, idx_t nr_lbas, backend_buf_ptr buffer, RequestType type)
	    : IORequest(lba_location, lba_size, nr_lbas, buffer, type) {
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
	AsyncIORequest(idx_t lba_location, idx_t lba_size, idx_t nr_lbas, backend_buf_ptr buffer, RequestType type)
	    : IORequest(lba_location, lba_size, nr_lbas, buffer, type), promise(make_uniq<std::promise<bool>>()),
	      future(promise->get_future()) {
		// Constructor implementation
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
