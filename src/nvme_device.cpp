#include "nvme_device.hpp"

namespace duckdb {
NvmeDevice::NvmeDevice(const string &device_path, const idx_t placement_handles)
    : dev_path(device_path), plhdls(placement_handles) {
	xnvme_opts opts = xnvme_opts_default();
	device = xnvme_dev_open(device_path.c_str(), &opts);
	if (!device) {
		xnvme_cli_perr("xnvme_dev_open()", errno);
		throw InternalException("Unable to open device");
	}

	allocated_placement_identifiers["nvmefs:///tmp"] = 1;
	geometry = LoadDeviceGeometry();
}

NvmeDevice::~NvmeDevice() {
	xnvme_dev_close(device);
}

idx_t NvmeDevice::Write(void *buffer, const CmdContext &context) {
	const NvmeCmdContext &ctx = static_cast<const NvmeCmdContext &>(context);
	D_ASSERT(ctx.nr_lbas > 0);
	// We only support offset writes within a single block
	D_ASSERT((ctx.offset == 0 && ctx.nr_lbas > 1) || (ctx.offset >= 0 && ctx.nr_lbas == 1));

	nvme_buf_ptr dev_buffer = AllocateDeviceBuffer(ctx.nr_bytes);
	if (ctx.offset > 0) {
		// Check if write is fully contained within single block
		D_ASSERT(ctx.offset + ctx.nr_bytes < geometry.lba_size);
		// Read the whole LBA block
		Read(dev_buffer, ctx);
	}
	memcpy(dev_buffer, (char *)buffer + ctx.offset, ctx.nr_bytes);

	uint32_t nsid = xnvme_dev_get_nsid(device);
	uint8_t plid_idx = GetPlacementIdentifierOrDefault(ctx.filepath);
	xnvme_cmd_ctx xnvme_ctx = PrepareWriteContext(plid_idx);

	int err = xnvme_nvm_write(&xnvme_ctx, nsid, ctx.start_lba, ctx.nr_lbas - 1, dev_buffer, nullptr);
	if (err) {
		xnvme_cli_perr("Could not write to device with xnvme_nvme_write(): ", err);
		throw IOException("Encountered error when writing to NVMe device");
	}

	FreeDeviceBuffer(dev_buffer);

	return ctx.nr_lbas;
}

idx_t NvmeDevice::Read(void *buffer, const CmdContext &context) {
	const NvmeCmdContext &ctx = static_cast<const NvmeCmdContext &>(context);
	D_ASSERT(ctx.nr_lbas > 0);
	// We only support offset reads within a single block
	D_ASSERT((ctx.offset == 0 && ctx.nr_lbas > 1) || (ctx.offset >= 0 && ctx.nr_lbas == 1));

	nvme_buf_ptr dev_buffer = AllocateDeviceBuffer(ctx.nr_bytes);

	uint32_t nsid = xnvme_dev_get_nsid(device);
	uint8_t plid_idx = GetPlacementIdentifierOrDefault(ctx.filepath);
	xnvme_cmd_ctx xnvme_ctx = PrepareReadContext(plid_idx);

	int err = xnvme_nvm_read(&xnvme_ctx, nsid, ctx.start_lba, ctx.nr_lbas - 1, dev_buffer, nullptr);
	if (err) {
		xnvme_cli_perr("Could not write to device with xnvme_nvme_write(): ", err);
		throw IOException("Encountered error when writing to NVMe device");
	}

	memcpy(buffer, (char *)dev_buffer + ctx.offset, ctx.nr_bytes);

	FreeDeviceBuffer(dev_buffer);

	return ctx.nr_lbas;
}

DeviceGeometry NvmeDevice::GetDeviceGeometry() {
	return geometry;
}

uint8_t NvmeDevice::GetPlacementIdentifierOrDefault(const string &path) {
	uint8_t placement_identifier = 0;
	for (const auto &kv : allocated_placement_identifiers) {
		if (StringUtil::StartsWith(path, kv.first)) {
			placement_identifier = kv.second;
		}
	}

	return placement_identifier;
}

nvme_buf_ptr NvmeDevice::AllocateDeviceBuffer(idx_t nr_bytes) {
	return xnvme_buf_alloc(device, nr_bytes);
}

void NvmeDevice::FreeDeviceBuffer(nvme_buf_ptr buffer) {
	xnvme_buf_free(device, buffer);
}

DeviceGeometry NvmeDevice::LoadDeviceGeometry() {
	NvmeDeviceGeometry geometry {};

	const xnvme_geo *geo = xnvme_dev_get_geo(device);
	const xnvme_spec_idfy_ns *nsgeo = xnvme_dev_get_ns(device);

	geometry.lba_size = geo->lba_nbytes;
	geometry.lba_count = nsgeo->nsze;

	return geometry;
}

xnvme_cmd_ctx NvmeDevice::PrepareWriteContext(idx_t plid_idx) {
	xnvme_cmd_ctx ctx = xnvme_cmd_ctx_from_dev(device);
	uint32_t nsid = xnvme_dev_get_nsid(device);

	// Retrieve information about recliam unit handles
	struct xnvme_spec_ruhs *ruhs = nullptr;
	// TODO: verify this calculation!!
	uint32_t ruhs_nbytes = sizeof(*ruhs) + plhdls + sizeof(struct xnvme_spec_ruhs_desc);
	ruhs = (struct xnvme_spec_ruhs *)xnvme_buf_alloc(device, ruhs_nbytes);
	memset(ruhs, 0, ruhs_nbytes);
	xnvme_nvm_mgmt_recv(&ctx, nsid, XNVME_SPEC_IO_MGMT_RECV_RUHS, 0, ruhs, ruhs_nbytes);

	uint16_t phid = ruhs->desc[plid_idx].pi;
	ctx.cmd.common.cdw13 = phid << 16;

	xnvme_buf_free(device, ruhs);

	return ctx;
}

xnvme_cmd_ctx NvmeDevice::PrepareReadContext(idx_t plid_idx) {
	xnvme_cmd_ctx ctx = xnvme_cmd_ctx_from_dev(device);
	uint32_t nsid = xnvme_dev_get_nsid(device);

	// Retrieve information about recliam unit handles
	struct xnvme_spec_ruhs *ruhs = nullptr;
	// TODO: verify this calculation!!
	uint32_t ruhs_nbytes = sizeof(*ruhs) + plhdls + sizeof(struct xnvme_spec_ruhs_desc);
	ruhs = (struct xnvme_spec_ruhs *)xnvme_buf_alloc(device, ruhs_nbytes);
	memset(ruhs, 0, ruhs_nbytes);
	xnvme_nvm_mgmt_recv(&ctx, nsid, XNVME_SPEC_IO_MGMT_RECV_RUHS, 0, ruhs, ruhs_nbytes);

	uint16_t phid = ruhs->desc[plid_idx].pi;
	ctx.cmd.common.cdw13 = phid << 16;

	xnvme_buf_free(device, ruhs);

	return ctx;
}
} // namespace duckdb
