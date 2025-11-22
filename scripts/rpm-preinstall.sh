#!/bin/bash

# Create group if not exists
getent group xrootd-monitoring-shoveler >/dev/null || groupadd -r xrootd-monitoring-shoveler

# Verify group was created
if ! getent group xrootd-monitoring-shoveler >/dev/null; then
   echo "ERROR: Failed to create group xrootd-monitoring-shoveler"
   exit 1
fi

# Create user if not exists
getent passwd xrootd-monitoring-shoveler >/dev/null || \
       useradd -r -g xrootd-monitoring-shoveler -c "XRootD monitoring shoveler user" \
       -s /sbin/nologin -d /var/spool/xrootd-monitoring-shoveler xrootd-monitoring-shoveler

# Verify user was created
if ! getent passwd xrootd-monitoring-shoveler >/dev/null; then
   echo "ERROR: Failed to create user xrootd-monitoring-shoveler"
   exit 1
fi
