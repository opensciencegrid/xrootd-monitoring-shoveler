#!/bin/bash

# Create group if not exists
if ! getent group xrootd-monitoring-shoveler >/dev/null; then
   if groupadd -r xrootd-monitoring-shoveler 2>&1; then
      echo "Created group xrootd-monitoring-shoveler"
   else
      # Check if group now exists (might have been created despite error message)
      if ! getent group xrootd-monitoring-shoveler >/dev/null; then
         echo "ERROR: Failed to create group xrootd-monitoring-shoveler"
         exit 1
      fi
   fi
fi

# Create user if not exists
if ! getent passwd xrootd-monitoring-shoveler >/dev/null; then
   if useradd -r -g xrootd-monitoring-shoveler -c "XRootD monitoring shoveler user" \
       -s /sbin/nologin -d /var/spool/xrootd-monitoring-shoveler xrootd-monitoring-shoveler 2>&1; then
      echo "Created user xrootd-monitoring-shoveler"
   else
      # Check if user now exists (might have been created despite error message)
      if ! getent passwd xrootd-monitoring-shoveler >/dev/null; then
         echo "ERROR: Failed to create user xrootd-monitoring-shoveler"
         exit 1
      fi
   fi
fi
