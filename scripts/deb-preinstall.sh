#!/bin/bash

SERVER_HOME=/var/spool/xrootd-monitoring-shoveler
SERVER_USER=xrootd-monitoring-shoveler
SERVER_GROUP=xrootd-monitoring-shoveler
SERVER_NAME="XRootD monitoring shoveler user"

# create user to avoid running server as root
# 1. create group if not existing
if ! getent group "$SERVER_GROUP" >/dev/null; then
   echo -n "Adding group $SERVER_GROUP.."
   addgroup --quiet --system $SERVER_GROUP 2>/dev/null ||true
   echo "..done"
fi
# 1. create user if not existing
if ! getent passwd $SERVER_USER >/dev/null; then
  echo -n "Adding system user $SERVER_USER.."
  adduser --quiet \
          --system \
          --ingroup $SERVER_GROUP \
          --no-create-home \
          --disabled-password \
          $SERVER_USER 2>/dev/null || true
  echo "..done"
fi
# 3. adjust passwd entry
usermod -c "$SERVER_NAME" \
        -d $SERVER_HOME   \
        -g $SERVER_GROUP  \
           $SERVER_USER
