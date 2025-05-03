#include "duckdb.hpp"
#include <future>

namespace duckdb {

enum RequestType { READ, WRITE };

class IORequest {
public:
	IORequest(idx_t lba_location, idx_t lba_count, idx_t lba_size, void *buffer, RequestType type)
	    : lba_location(lba_location), lba_count(lba_count), lba_size(lba_size), buffer(buffer), type(type) {
	}

	virtual bool WaitForCompletion();

	void *GetBuffer() {
		return buffer;
	}

	idx_t GetBufferSize() {
		return lba_count * lba_size;
	}

private:
	idx_t lba_location;
	idx_t lba_count;
	idx_t lba_size;
	RequestType type;
	void *buffer;
};

class SyncIoRequest : public IORequest {
public:
	SyncIoRequest(idx_t lba_location, idx_t lba_count, idx_t lba_size, void *buffer, RequestType type)
	    : IORequest(lba_location, lba_count, lba_size, buffer, type) {
		// Constructor implementation
	}

	/// @brief Just returns true. Assumes that if an error has happend it was at the submission of the request.
	/// @return True
	bool WaitForCompletion() override {
		// Implementation for synchronous request completion
		return true;
	}
};

class AsyncIoRequest : public IORequest {
public:
	AsyncIoRequest(idx_t lba_location, idx_t lba_count, idx_t lba_size, void *buffer, RequestType type)
	    : IORequest(lba_location, lba_count, lba_size, buffer, type), promise(make_uniq<std::promise<bool>>()),
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
