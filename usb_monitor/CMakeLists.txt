cmake_minimum_required(VERSION 2.8.3)
project(usb_monitor)

find_package(catkin REQUIRED COMPONENTS
     rospy
     usb_monitor_msgs
)

catkin_python_setup()

catkin_package(
    INCLUDE_DIRS

    LIBRARIES

    CATKIN_DEPENDS
    rospy
    usb_monitor_msgs

    )


#############
## Install ##
#############


install(DIRECTORY launch/
  DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION}/launch
  )

install(PROGRAMS scripts/usb_monitor_node.py
  DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION}
  )

install(DIRECTORY src/
  DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION}/scripts/
  )
