#!/usr/bin/env python

import os

USB_FILE_SYSTEM_NOTIFICATION_TOPIC = "usb_file_system_notification"
USB_FILE_SYSTEM_SUBSCRIBE_SERVICE_NAME = "usb_file_system_subscribe"
USB_MOUNT_POINT_MANAGER_SERVICE_NAME = "usb_mount_point_manager"

# Base mount point.
BASE_MOUNT_POINT = os.path.join(os.sep, "mnt", "usb_monitor")
