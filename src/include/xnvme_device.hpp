#include "device.hpp"
#include "duckdb.hpp"
#include "io_backend.hpp"

namespace duckdb {
class XNVMeDevice : public Device {
public:
	XNVMeDevice(const string &device_path, const idx_t placement_handles, const string &backend, const bool async);
	~XNVMeDevice() override;

	idx_t Write(void *buffer, const CmdContext &context) override;
	idx_t Read(void *buffer, const CmdContext &context) override;

	string GetName() const override {
		return "XNVMeDevice";
	}

	DeviceGeometry GetDeviceGeometry() override;

private:
	unique_ptr<IOBackend> backend;
};

} // namespace duckdb
