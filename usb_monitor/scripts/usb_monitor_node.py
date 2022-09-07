#!/usr/bin/env python

"""
usb_monitor_node.py

This module creates the usb_monitor_node which monitors the connection/disconnection
of the USB drive and provides functionality to watch for specific files/folders in it.
It provides services and functions to subscribe to be notified if a particular file/folder
is found in USB, publisher to broadcast details of the found files/folders and mount
point management functions.

The node defines:
    usb_file_system_notification_publisher: A publisher to broadcast the
                                            notification messages when any of the
                                            files/folders that are in the watchlist are
                                            found in the USB drive.
    usb_file_system_subscriber_service: A service to add files/folders to the watchlist
                                        so that when they are found in the USB drive,
                                        a file system notification will be triggered.
    usb_mount_point_manager_service: A service exposing the functionality to safely
                                     increment/decrement the reference counter for
                                     the mount points.
"""

import os
import threading
import Queue
import pyudev
import psutil
import rospy

from usb_monitor_msgs.srv import (USBFileSystemSubscribe,
                                  USBFileSystemSubscribeResponse,
                                  USBMountPointManager,
                                  USBMountPointManagerResponse)

from usb_monitor_msgs.msg import USBFileSystemNotification
from usb_monitor import (constants,
                             mount_point_tracker)

#########################################################################################
# USB event monitor.


class USBMonitorNode:
    """Node responsible for managing and monitoring USB connections and notify if a
       watched file/folder is present.
    """

    def __init__(self):
        """Create a USBMonitorNode.
        """
        rospy.init_node("usb_monitor_node")
        rospy.loginfo("usb_monitor_node started")
        self.queue = Queue.Queue()
        self.stop_queue = threading.Event()
        self.mount_point_map = dict()
        self.subscribers = list()

        # Service to add files/folders to the watchlist (subscribers).
        self.file_system_service_cb_group = None
        self.usb_file_system_subscriber_service = \
            self.create_service(USBFileSystemSubscribe,
                                "~"+constants.USB_FILE_SYSTEM_SUBSCRIBE_SERVICE_NAME,
                                self.usb_file_system_subscribe_cb)

        # Service exposing the functionality to safely increment/decrement the refernce
        # counter for the mount poUSB_FILE_SYSTEM_SUBSCRIBE_SERVICE_NAMEint.
        self.usb_mpm_cb_group = None
        self.usb_mount_point_manager_service = \
            self.create_service(USBMountPointManager,
                                "~"+constants.USB_MOUNT_POINT_MANAGER_SERVICE_NAME,
                                self.usb_mount_point_manager_cb)

        # Publisher to broadcast the notification messages.
        self.usb_file_system_notif_cb = None
        self.usb_file_system_notification_publisher = \
            self.create_publisher(USBFileSystemNotification,
                                  "~"+constants.USB_FILE_SYSTEM_NOTIFICATION_TOPIC,
                                  10
                                  )

        # Heartbeat timer.
        self.timer_count = 0
        self.timer = self.create_timer(5.0, self.timer_callback)
        rospy.loginfo("USB Monitor node successfully created")

    def create_service(self,service_type,service_name,service_callback):
        return rospy.Service(service_name, service_type, service_callback)

    def create_publisher(self,topic_type,topic_name,queue_size):
        return rospy.Publisher(topic_name,topic_type,queue_size=queue_size)

    def create_timer(self,duration,time_callback):
        return rospy.Timer(rospy.Duration(duration), time_callback)

    def timer_callback(self,event):
        """Heartbeat function to keep the node alive.
        """
        rospy.logdebug("Timer heartbeat %d",self.timer_count)
        self.timer_count += 1

    def __enter__(self):
        """Called when the node object is created using the 'with' statement.

        Returns:
           USBMonitorNode : self object returned.
        """
        self.thread = threading.Thread(target=self.processor)
        self.thread.start()

        self.pd_context = pyudev.Context()
        try:
            monitor = pyudev.Monitor.from_netlink(self.pd_context)
            monitor.filter_by(subsystem="block")
            self.observer = pyudev.MonitorObserver(monitor, callback=self.block_device_monitor)

        except Exception as ex:
            rospy.loginfo("Failed to create UDEV monitor: ",ex)
            self.observer = None

        # Start USB event monitor.
        self.start()
        return self

    def __exit__(self, ExcType, ExcValue, Traceback):
        """Called when the object is destroyed.
        """
        self.stop()
        self.stop_queue.set()
        self.queue.put(None)
        self.thread.join()

    def usb_mount_point_manager_cb(self, req):
        """Callback for the usb_mount_point_manager service. Executes increment/decrement
           actions for the mount point node name passed.

        Args:
            req (USBMountPointManagerSrv.Request): Request object with node_name(str)
                                                   identifying the mount point and the
                                                   action(int) flag.
            res (USBMountPointManagerSrv.Response): Response object with error(int) flag
                                                    to indicate if the service call was
                                                    successful.

        Returns:
            USBMountPointManagerSrv.Response: Response object with error(int) flag to
                                              indicate if the service call was successful.
        """
        node_name = req.node_name
        action = req.action
        res=USBMountPointManagerResponse()
        res.error = 0
        rospy.loginfo("USB mount point manager callback: %s %d",node_name,action)
        if node_name in self.mount_point_map:
            if action == 0:
                self.mount_point_map[node_name].ref_dec()
            elif action == 1:
                self.mount_point_map[node_name].ref_inc()
            else:
                res.error = 1
                rospy.logerr("Incorrect action value: %d",action)
        else:
            res.error = 1
            rospy.logerr("Node name not found: %s",node_name)
        return res

    def usb_file_system_subscribe_cb(self, req):
        """Callback for the usb_file_system_subscribe service. Adds the file/folder details
           to the subscriber list.

        Args:
            req (USBFileSystemSubscribeSrv.Request): Request object with file_name(str),
                                                     callback_name(str) and
                                                     verify_name_exists(bool) flag to be
                                                     added to the subscriber list.
            res (USBFileSystemSubscribeSrv.Response): Response object with error(int) flag
                                                      to indicate if the service call was
                                                      successful.

        Returns:
            USBFileSystemSubscribeSrv.Response: Response object with error(int) flag to
                                                indicate if the service call was successful.
        """
        self.subscribers.append((req.file_name, req.callback_name, req.verify_name_exists))
        res=USBFileSystemSubscribeResponse()
        res.error = 0
        rospy.loginfo("USB File system subscription : %s %s %s"
                               ,req.file_name
                               ,req.callback_name
                               ,req.verify_name_exists)
        self.add_already_connected_devices_to_queue()
        return res

    def processor(self):
        """Main daemon thread that is checking for USB connection and processing its contents.
        """
        while not self.stop_queue.isSet():
            device = self.queue.get()
            if device is None:
                continue

            # Look for block devices.
            if device.subsystem != "block":
                continue

            # Look for partition/disk devices.
            if device.device_type not in ("partition", "disk"):
                continue

            # Look for SCSI disk devices (0-15).
            if device["MAJOR"] != "8":
                continue

            rospy.loginfo("Detected file system %s",device.device_node)
            self.process(device.device_node)
            #self.is_already_mounted()


            rospy.loginfo("After processing : %s",device.device_node)

    def is_already_mounted(self):
        context = pyudev.Context()

        removable = [device for device in context.list_devices(subsystem='block', DEVTYPE='disk') if device.attributes.asstring('removable') == "1"]
        for device in removable:
            partitions = [device.device_node for device in context.list_devices(subsystem='block', DEVTYPE='partition', parent=device)]
            print("All removable partitions: {}".format(", ".join(partitions)))
            print("Mounted removable partitions:")
            print(psutil.disk_partitions())
            for p in psutil.disk_partitions():
                if p.device in partitions:
                    print("  {}: {}".format(p.device, p.mountpoint))

    def process(self, filesystem, post_action=None):
        """Helper function to parse the contents of the USB drive and publish notification
           messages for watched files/folders.

        Args:
            filesystem (str): Filesystem path where teh device is mounted.
            post_action (function, optional): Any function that needs to be executed
                                              while decrementing the reference for the
                                              filesystem after completing processing the
                                              file/folder.
                                              Defaults to None.

        Returns:
            bool: True if successfully processed the filesystem else False.
        """
        # Mount the filesystem and get the mount point.
        mount_point = self.get_mount_point(filesystem, post_action)
        if mount_point.name == "":
            return False

        # Check if any of the watched files are present.
        for file_name, callback_name, verify_name_exists in self.subscribers:
            full_name = os.path.join(mount_point.name, file_name)

            if verify_name_exists and \
               (not os.path.isdir(full_name)
                    and not os.path.isfile(full_name)):
                rospy.loginfo("verify_name_exists : %s %s %s"
                                       , full_name
                                       , os.path.isdir(full_name)
                                       , full_name)
                continue

            mount_point.ref_inc()
            notification_msg = USBFileSystemNotification()
            notification_msg.path = mount_point.name
            notification_msg.file_name = file_name
            notification_msg.callback_name = callback_name
            notification_msg.node_name = filesystem
            rospy.loginfo("USB File system notification: %s %s %s %s"
                                   ,notification_msg.path
                                   ,notification_msg.file_name
                                   ,notification_msg.node_name
                                   ,notification_msg.callback_name)

            self.usb_file_system_notification_publisher.publish(notification_msg)

        # Dereference mount point.
        mount_point.ref_dec()
        return True

    def block_device_monitor(self, device):
        """Function callback passed to pyudev.MonitorObserver to monitor USB connection.

        Args:
            device (pyudev.Device): USB Device with attached attributes and properties.
        """
        # Only care about new insertions.
        if device.action == "add":
            self.queue.put(device)
        elif device.action == "remove":
            rospy.loginfo("Removed file system %s",device.device_node)
            if device.device_node in self.mount_point_map:
                self.mount_point_map[device.device_node].ref_dec()
                #rospy.loginfo("%s",device)

    def add_already_connected_devices_to_queue(self):
        # Enumerate and process already connected media before enabling the monitor.
        for device in self.pd_context.list_devices(subsystem="block"):
            self.queue.put(device)

    def start(self):
        """Initializing function to start monitoring.
        """
        self.add_already_connected_devices_to_queue()

        if self.observer is not None:
            self.observer.start()

    def stop(self):
        """Function to stop monitoring.
        """
        if self.observer is not None:
            self.observer.stop()

    def get_mount_point(self, node_name, post_action=None):
        """Return a MountPoint object from the cache or create one if it does not exist.

        Args:
            node_name (str): Name of the node mounted.
            post_action (function, optional): Optional post action function.
                                              Defaults to None.

        Returns:
            MountPoint: Mount point tracker object.
        """
        try:
            mount_point = self.mount_point_map[node_name]
            if not mount_point.is_valid():
                raise Exception("Not a valid mount point.")

            rospy.loginfo("%s is previosuly mounted",node_name )
            mount_point.ref_inc()
            mount_point.add_post_action(post_action)

        except Exception:
            rospy.loginfo("Creating a mount point for %s",node_name)
            mount_point = mount_point_tracker.MountPoint(node_name,
                                                         
                                                         post_action)
            self.mount_point_map[node_name] = mount_point

        return mount_point


def main(args=None):
    with USBMonitorNode() as usb_monitor_node:
        rate = rospy.Rate(10) # 10hz
        while not rospy.is_shutdown():
            rate.sleep()
        


if __name__ == "__main__":
    main()
