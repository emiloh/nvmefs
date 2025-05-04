import os
import pathlib
import subprocess
from pathlib import Path

class NvmeDeviceNamespace:
    def __init__(self, device_path: str, namespace_id: int, number_of_blocks: int, is_mounted: bool = False):
        self.base_device_path = device_path
        self.namespace_id = namespace_id
        self.is_mounted = is_mounted
        self.block_size = 4096
        self.device_id = int(device_path[-1])

        # TODO: Document environment variables in readme
        self.log_id = log_id
        self.sent_offset = sent_offset
        self.written_offset = written_offset

        self.number_of_blocks = number_of_blocks

    def delete(self):
        """
        Deletes a namespace on the device. After this is called the namespace is no longer usable
        """
        if self.is_mounted:
            os.system(f"umount -l {self.get_device_path()}")

        os.system(f"nvme delete-ns {self.base_device_path} --namespace-id={self.namespace_id}")

    def deallocate_blocks(self):
        """
        Deallocates all blocks on the device
        """

        os.system(f"nvme dsm {self.base_device_path} --namespace-id={self.namespace_id} --ad -s 0 -b {self.number_of_blocks}")
    
    def get_generic_device_path(self):
        """
        Returns the generic device path for the namespace
        """
        return f"/dev/ng{self.device_id}n{self.namespace_id}"
    
    def get_device_path(self):
        """
        Returns the device path for the namespace
        """
        return f"/dev/nvme{self.device_id}n{self.namespace_id}"
    

    def get_written_bytes(self):
        cmd = f"""nvme get-log {self.get_device_path()} --log-id={self.log_id} --log-len=512 -b"""
        res = subprocess.check_output(cmd, shell=True)
        host_written = int.from_bytes(res[self.sent_offset[0]:self.sent_offset[1]+1], byteorder="little") 
        media_written = int.from_bytes(res[self.written_offset[0]:self.written_offset[1]+1], byteorder="little") 

        if host_written == 0: return (0,0)

        return (host_written, media_written)
    

class NvmeDevice:
    """
    Represents an NVMe device. This class is used to interact with the administrative interface of the NVMe device using the nvme client.
    This is without the namespace suffix,e.g. /dev/nvme0
    """
    def __init__(self, device_path: str):
        self.namespaces = []
        self.base_device_path = device_path
        self.block_size = 4096
        self.device_id = int(device_path[-1])

        if self.log_id is None :
            raise Exception("Environment variable LOGIDWAF")

        self.number_of_blocks = self.__get_device_info(device_path)
    
    def __get_device_info(self, device_path: str):
        command = f"nvme id-ctrl {device_path} | grep 'tnvmcap' | sed 's/,//g' | awk -v BS={self.block_size} '{{print $3/BS}}'"
        block_output = subprocess.check_output(command, shell=True)
        number_of_blocks = int(block_output)

        return number_of_blocks
    
    def get_ns_block_amount(self, namespace_id: int):
        """
        Returns the number of blocks in the namespace
        """
        for namespace in self.namespaces:
            if namespace.namespace_id == namespace_id:
                return namespace.number_of_blocks
        
        command = f"nvme id-ns {self.base_device_path} -n {namespace_id} | grep 'nvmcap' | sed 's/,//g' | awk -v BS={self.block_size} '{{print $3/BS}}'"
        block_output = subprocess.check_output(command, shell=True)
        number_of_blocks = int(block_output) 

        return number_of_blocks

    def deallocate(self, namespace: NvmeDeviceNamespace):
        """
        Deallocates all blocks on the device
        """

        namespace.deallocate_blocks()
    
    def deallocate_nsid(self, namespace_id: int):
        """
        Deallocates all blocks on the device
        """
        for namespace in self.namespaces:
            if namespace.namespace_id == namespace_id:
                namespace.deallocate_blocks()
                return
        
        number_of_blocks = self.get_ns_block_amount(namespace_id)
        os.system(f"nvme dsm {self.base_device_path} --namespace-id={namespace_id} --ad -s 0 -b {number_of_blocks}")


    def enable_fdp(self):
        """
        Enables flexible data placement(FDP) on the device
        """

        os.system(f"nvme set-feature {self.base_device_path} -f 0x1D -c 1 -s")

    def disable_fdp(self):
        """
        Disables flexible data placement(FDP) on the device
        """
        
        os.system(f"nvme set-feature {self.base_device_path} -f 0x1D -c 0 -s")

    def delete_namespace(self, namespace: NvmeDeviceNamespace):
        """
        Deletes a namespace on the device
        """

        namespace.delete()
    
    def delete_namespace_nsid(self, namespace_id: int):
        """
        Deletes a namespace on the device
        """

        for namespace in self.namespaces:
            if namespace.namespace_id == namespace_id:
                namespace.delete()
                return
        
        os.system(f"nvme delete-ns {self.base_device_path} --namespace-id={namespace_id}")

    def create_namespace(self, device_path: str, namespace_id: int, enable_fdp: bool = False, size: float = 1.0, mount_path:str = None):
        """
        Creates a namespace on the device and attaches it

        :param device_path: The path to the device
        :param namespace_id: The ID of the namespace to create
        :param enable_fdp: Whether to enable flexible data placement
        :param size: The size in percentage(in a range [1,0]) of the device to use for the namespace. Example size of 50% is 0.5
        """

        # Create a namespace on the device
        result = 1
        ns_number_of_blocks = int(self.number_of_blocks*size)
        
        if enable_fdp:
            result = os.system(f"nvme create-ns {device_path} -b {self.block_size} --nsze={ns_number_of_blocks} --ncap={ns_number_of_blocks} --nphndls=7 --phndls=0,1,2,3,4,5,6")
        else: 
            result = os.system(f"nvme create-ns {device_path} -b {self.block_size} --nsze={ns_number_of_blocks} --ncap={ns_number_of_blocks}")

        if result != 0:
            raise Exception("Failed to create namespace")

        # Attach the namespace to the device
        result = os.system(f"nvme attach-ns {device_path} --namespace-id={namespace_id} --controllers=0x7")

        if result != 0:
            raise Exception("Failed to attach namespace")
        
        is_mounted = mount_path is not None
        new_namespace = NvmeDeviceNamespace(device_path, namespace_id, ns_number_of_blocks, is_mounted)
        self.namespaces.append(new_namespace)

        if is_mounted:
            os.system(f"mkfs.ext4 {new_namespace.get_device_path()} -b {self.block_size} {ns_number_of_blocks}") # Format the device namespace
            result = os.system(f"mount {new_namespace.get_device_path()} {mount_path}") # Mount the device namespace to a mount path

            if result != 0:
                raise Exception("Failed to mount namespace")
        
        return new_namespace

    def reset(self):
        """
        Reset the device by deleting all namespaces and unmounting mounted namespaces
        """
        
        for namespace in self.namespaces:
            namespace.deallocate_blocks()
            namespace.delete()

def setup_device(device: NvmeDevice, namespace_id:int = 1, enable_fdp: bool = False, mount_path: str = None) -> NvmeDeviceNamespace:
    """
    Sets up the device by creating a namespace and enabling FDP if required
    """

    # TODO: Check if unknown namespace is already mounted and unmount before dealocating and delete of ns
    device_ns_path = pathlib.Path(f"{device.base_device_path}n{namespace_id}")

    if device_ns_path.exists():
        device.deallocate_nsid(namespace_id)
        device.delete_namespace_nsid(namespace_id)
    
    # Ensure that FDP is enabled / disabled
    if enable_fdp:
        device.enable_fdp()
    else:
        device.disable_fdp()
    
    # Create new namespace with a new configuration
    return device.create_namespace(device.base_device_path, namespace_id, enable_fdp, mount_path=mount_path)