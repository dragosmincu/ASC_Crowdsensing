"""
This module represents a device.

Computer Systems Architecture Course
Assignment 1
March 2016
"""

from threading import Event, Thread, BoundedSemaphore, Lock
from cond_barrier import ReusableBarrier


class Device(object):
    """
    Class that represents a device.
    """
    barrier = None
    barrier_event = Event()

    def __init__(self, device_id, sensor_data, supervisor):
        """
        Constructor.

        @type device_id: Integer
        @param device_id: the unique id of this node; between 0 and N-1

        @type sensor_data: List of (Integer, Float)
        @param sensor_data: a list containing (location, data) as measured by this device

        @type supervisor: Supervisor
        @param supervisor: the testing infrastructure's control and validation component
        """
        self.device_id = device_id
        self.sensor_data = sensor_data
        self.supervisor = supervisor

        # variables for script threads
        self.scripts = []
        self.scripts_semaphore = BoundedSemaphore(8)
        self.scripts_lock = Lock()

        # events for scripts and timepoints
        self.script_received = Event()
        self.timepoint_done = Event()

        self.thread = DeviceThread(self)
        self.thread.start()

    def __str__(self):
        """
        Pretty prints this device.

        @rtype: String
        @return: a string containing the id of this device
        """
        return "Device %d" % self.device_id

    def setup_devices(self, devices):
        """
        Setup the devices before simulation begins.

        @type devices: List of Device
        @param devices: list containing all devices
        """
        # barrier initialization
        if Device.barrier is None and self.device_id == 0:
            Device.barrier = ReusableBarrier(len(devices))
            Device.barrier_event.set()

    def assign_script(self, script, location):
        """
        Provide a script for the device to execute.

        @type script: Script
        @param script: the script to execute from now on at each timepoint; None if the
            current timepoint has ended

        @type location: Integer
        @param location: the location for which the script is interested in
        """
        # receiving new scripts
        with self.scripts_lock:
            self.script_received.set()
            if script is not None:
                self.scripts.append((script, location))
            else:
                self.timepoint_done.set()

    def get_data(self, location):
        """
        Returns the pollution value this device has for the given location.

        @type location: Integer
        @param location: a location for which obtain the data

        @rtype: Float
        @return: the pollution value
        """
        return self.sensor_data[location] if location in self.sensor_data else None

    def set_data(self, location, data):
        """
        Sets the pollution value stored by this device for the given location.

        @type location: Integer
        @param location: a location for which to set the data

        @type data: Float
        @param data: the pollution value
        """
        if location in self.sensor_data:
            self.sensor_data[location] = data

    def shutdown(self):
        """
        Instructs the device to shutdown (terminate all threads). This method
        is invoked by the tester. This method must block until all the threads
        started by this device terminate.
        """
        self.thread.join()


class DeviceThread(Thread):
    """
    Class that implements the device's worker thread.
    """

    def __init__(self, device):
        """
        Constructor.

        @type device: Device
        @param device: the device which owns this thread
        """
        Thread.__init__(self, name="Device Thread %d" % device.device_id)
        self.device = device

    def run(self):
        Device.barrier_event.wait()
        while True:
            neighbours = self.device.supervisor.get_neighbours()

            if neighbours is None:
                break

            # the start of a new timepoint
            script_index = 0
            script_threads = []
            while True:
                self.device.scripts_lock.acquire()
                if script_index < len(self.device.scripts):
                    # launching another thread if possible
                    self.device.scripts_lock.release()
                    self.device.scripts_semaphore.acquire()

                    script_threads.append(ScriptThread(self.device, self.device.scripts[script_index][0],
                                                       self.device.scripts[script_index][1], neighbours))
                    script_threads[-1].start()

                    script_index += 1
                else:
                    # checking if the timepoint is done
                    if self.device.timepoint_done.is_set() and script_index == len(self.device.scripts):
                        self.device.timepoint_done.clear()
                        self.device.scripts_lock.release()
                        break
                    else:
                        # waiting for more scripts
                        self.device.scripts_lock.release()
                        self.device.script_received.wait()
                        self.device.scripts_lock.acquire()
                        self.device.script_received.clear()
                        self.device.scripts_lock.release()

            # waiting for the scripts to finish
            for script_thread in script_threads:
                script_thread.join()

            # barrier for timepoint sincronization
            Device.barrier.wait()


class ScriptThread(Thread):
    """
    Class that implements parallel script execution.
    Contains locks for locations.
    """
    locations_locks = {}

    def __init__(self, device, script, location, neighbours):
        """
        Constructor.

        @type device: Device
        @param device: The device who owns this thread
        @type script: Script
        @param script: The script to execute
        @type location: Integer
        @param location: The location the script is interested in
        @type neighbours: List
        @param neighbours: The list of neighbours the script should check for data
        """
        Thread.__init__(self)
        self.location = location
        self.script = script
        self.device = device
        self.neighbours = neighbours

        if location not in ScriptThread.locations_locks:
            ScriptThread.locations_locks[location] = Lock()

    def run(self):
        # locks the location
        with ScriptThread.locations_locks[self.location]:
            # collecting the data for the script
            script_data = []

            for device in self.neighbours:
                data = device.get_data(self.location)
                if data is not None:
                    script_data.append(data)

            data = self.device.get_data(self.location)
            if data is not None:
                script_data.append(data)

            # executing the script and updating devices data
            if script_data != []:
                result = self.script.run(script_data)
                for device in self.neighbours:
                    device.set_data(self.location, result)
                self.device.set_data(self.location, result)

        # signaling the end by using the semaphore
        self.device.scripts_semaphore.release()