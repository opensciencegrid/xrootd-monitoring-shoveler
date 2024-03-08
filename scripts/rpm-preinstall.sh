#!/bin/bash
getent group xrootd-monitoring-shoveler >/dev/null || groupadd -r xrootd-monitoring-shoveler
getent passwd xrootd-monitoring-shoveler >/dev/null || \
       useradd -r -g xrootd-monitoring-shoveler -c "XRootD monitoring shoveler user" \
       -s /sbin/nologin -d /var/spool/xrootd-monitoring-shoveler xrootd-monitoring-shoveler
