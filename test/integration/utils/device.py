
import os
import subprocess
from pathlib import Path

class NvmeDevice:
    """
    Represents an NVMe device. This class is used to interact with the administrative interface of the NVMe device using the nvme client.
    """
    def __init__(self, device_path: str):
        self.device_path = device_path
        self.block_size = 4096
        self.device_id = int(device_path[-1])

        self.number_of_blocks = self.__get_device_info(device_path)
    
    def __get_device_info(self, device_path: str):
        block_output = subprocess.check_output(["nvme", "id-ctrl", device_path, "|", "grep", "'tnvmcap'", "|", "sed", "'s/,//g'", "|", "awk", "-v", "BS="+self.block_size, "'{print $3/BS}'"])
        number_of_blocks = int(block_output)

        return number_of_blocks

        

    def deallocate(self, namespace_id: int):
        """
        Deallocates all blocks on the device
        """

        os.system("nvme dsm %s --namespace-id=%d --ad -s 0 -b %d", self.device_path, namespace_id, self.number_of_blocks)


    def enable_fdp(self):
        """
        Enables flexible data placement(FDP) on the device
        """
        os.system("nvme set-feature %s -f 0x1D -c 1 -s", self.device_path)

    def disable_fdp(self):
        """
        Disables flexible data placement(FDP) on the device
        """
        os.system("nvme set-feature %s -f 0x1D -c 0 -s", self.device_path)

    def delete_namespace(self, namespace_id: int):
        """
        Deletes a namespace on the device
        """
        os.system("nvme delete-ns %s --namespace-id=%d", self.device_path, namespace_id)

    def create_namespace(self, device_path: str, namespace_id: int, enable_fdp: bool = False):
        """
        Creates a namespace on the device and attaches it
        """

        # Create a namespace on the device
        result = 1
        if enable_fdp:
            result = os.system("nvme create-ns %s -b %d --nsze=%d --ncap=%d --nphndls=7 --phndls=0,1,2,3,4,5,6", device_path, self.block_size, self.number_of_blocks, self.number_of_blocks)
        else: 
            result = os.system("nvme create-ns %s -b %d --nsze=%d --ncap=%d", device_path, self.block_size, self.number_of_blocks, self.number_of_blocks)

        if result != 0:
            raise Exception("Failed to create namespace")

        # Attach the namespace to the device
        result = os.system("nvme attach-ns %s --namespace-id=%d --controllers=0x7")

        if result != 0:
            raise Exception("Failed to attach namespace")
        
        return self.device_path + namespace_id, "/dev/ng"+self.device_id+"n"+namespace_id

def setup_device(device: NvmeDevice, namespace_id:int = 1, enable_fdp: bool = False):
    """
    Sets up the device by creating a namespace and enabling FDP if required
    """

    device_ns_path = pathlib.Path("/dev/nvme1n%d", namespace_id)
    if device_ns_path.exists():
        device.deallocate(namespace_id)
        device.delete_namespace(namespace_id)
    
    # Ensure that FDP is enabled / disabled
    if enable_fdp:
        device.enable_fdp()
    else:
        device.disable_fdp()
    
    # Create new namespace with a new configuration
    return device.create_namespace(device.device_path, namespace_id, enable_fdp)