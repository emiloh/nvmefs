#include "io_request.hpp"
#include "concurrentqueue.h"
#include <libxnvme.h>

namespace duckdb {

typedef void *backend_buf_ptr;

class IOBackend {
public:
	IOBackend(const string &device_path, const idx_t placement_handles, const string &backend, bool async)
	    : dev_path(device_path), plhdls(placement_handles), backend(backend) {

		xnvme_opts opts = xnvme_opts_default();
		PrepareOpts(opts, async);
		device = xnvme_dev_open(device_path.c_str(), &opts);
		if (!device) {
			xnvme_cli_perr("xnvme_dev_open()", errno);
			throw InternalException("Unable to open device");
		}
	}

	virtual ~IOBackend() = default;

	/// @brief Submits an IO request to the backend. This function may be blocking until the request is actually
	/// submitted.
	/// @param request The request to be submitted.
	/// @return The number of LBA blocks that were submitted.
	virtual idx_t SubmitRequest(IORequest request) = 0;

	virtual IORequest CreateReadRequest(idx_t lba_location, idx_t lba_count);
	virtual IORequest CreateWriteRequest(idx_t lba_location, idx_t lba_count);

	virtual void Sync();

protected:
	/// @brief Allocates a backend specific buffer. Should be freed with FreeDeviceBuffer.
	/// @param nr_bytes The number of bytes to allocate (The allocated buffer might be larger)
	/// @return Pointer to allocated device buffer
	backend_buf_ptr AllocateBuffer(idx_t nr_bytes);

	/// @brief Frees a backend specific buffer
	/// @param buffer The buffer to free
	void FreeBuffer(backend_buf_ptr buffer);

	virtual string GetName();

protected:
	xnvme_dev *device;

private:
	void PrepareOpts(xnvme_opts &opts, bool async) {
		if (async) {
			opts.async = backend.data();
			if (StringUtil::Equals(backend.data(), "io_uring_cmd")) {
				opts.sync = "nvme";
			}
		} else {
			opts.sync = backend.data();
		}
	}

private:
	string dev_path;
	idx_t plhdls;
	string backend;
};

class SyncIOBackend : public IOBackend {

public:
	SyncIOBackend(const string &device_path, const idx_t placement_handles, const string &backend);

	idx_t SubmitRequest(IORequest request) override;

	IORequest CreateReadRequest(idx_t lba_location, idx_t lba_count) override;
	IORequest CreateWriteRequest(idx_t lba_location, idx_t lba_count) override;

	void Sync() override;

protected:
	string GetName() override {
		return "SyncIOBackend";
	}
};

class AsyncIOBackend : public IOBackend {
public:
	AsyncIOBackend(const string &device_path, const idx_t placement_handles, const string &backend);

	idx_t SubmitRequest(IORequest request) override;

	IORequest CreateReadRequest(idx_t lba_location, idx_t lba_count) override;
	IORequest CreateWriteRequest(idx_t lba_location, idx_t lba_count) override;

	void Sync() override; // TODO: How are we going to sync the async requests?

protected:
	string GetName() override {
		return "AsyncIOBackend";
	}

private:
	void RunEventLoop();
	void StopEventLoop();

private:
	duckdb_moodycamel::ConcurrentQueue<IORequest> request_queue;
};

} // namespace duckdb
