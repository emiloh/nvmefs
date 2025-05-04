#pragma once

#include "duckdb/common/map.hpp"
#include "duckdb/common/string_util.hpp"
#include "device.hpp"
#include <libxnvme.h>
#include <mutex>
#include <future>
#include <chrono>
#include "io_backend.hpp"

namespace duckdb {

typedef void *nvme_buf_ptr;
static constexpr idx_t XNVME_QUEUE_DEPTH = 1 << 4;
static constexpr std::chrono::milliseconds POKE_MAX_BACKOFF_TIME = std::chrono::milliseconds(200);

struct NvmeDeviceGeometry : public DeviceGeometry {};
struct NvmeCmdContext : public CmdContext {
	string filepath;
};

class NvmeDevice : public Device {
public:
	NvmeDevice(const string &device_path, const idx_t placement_handles, const string &backend, const bool async);
	~NvmeDevice();

	/// @brief Writes data from the input buffer to the device at the specified LBA position
	/// @param buffer The input buffer that contains data to be written
	/// @param nr_bytes The amount of bytes to write
	/// @param nr_lbas The amount of LBAs to write
	/// @param start_lab The LBA to start writing from
	/// @param offset An offset into the LBA
	/// @return The amount of LBAs written to the device
	idx_t Write(void *buffer, const CmdContext &context) override;

	/// @brief Reads data from the device at the specified LBA position into the output buffer
	/// @param buffer The output buffer that will contain data read from the device
	/// @param nr_bytes The amount of bytes to read
	/// @param nr_lbas The amount of LBAs to read
	/// @param start_lab The LBA to start reading from
	/// @param offset An offset into the LBA
	/// @return The amount of LBAs read from the device
	idx_t Read(void *buffer, const CmdContext &context) override;

	/// @brief Fetches the geometry of the device
	/// @return The device geometry
	DeviceGeometry GetDeviceGeometry() override;

	/// @brief Get the name of the device
	/// @return Name of device
	string GetName() const {
		return "NvmeDevice";
	}

private:
	/// @brief Determines which placment handler should be used for the given path
	/// @param path The path of the file that will be opened
	/// @return A placement identifier
	uint8_t GetPlacementIdentifierOrDefault(const string &path);

	/// @brief Loads the geometry of the decvice
	/// @return The device geometry
	DeviceGeometry LoadDeviceGeometry();

private:
	map<string, uint8_t> allocated_placement_identifiers;
	unique_ptr<IOBackend> backend;
	const string dev_path;
	const idx_t plhdls;
	DeviceGeometry geometry;
};

} // namespace duckdb
