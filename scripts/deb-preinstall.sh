#!/bin/bash

SERVER_HOME=/var/spool/xrootd-monitoring-shoveler
SERVER_USER=xrootd-monitoring-shoveler
SERVER_GROUP=xrootd-monitoring-shoveler
SERVER_NAME="XRootD monitoring shoveler user"

# create user to avoid running server as root
# 1. create group if not existing
if ! getent group "$SERVER_GROUP" >/dev/null; then
   echo -n "Adding group $SERVER_GROUP.."
   # Try addgroup first (Debian/Ubuntu), fall back to groupadd (universal)
   if command -v addgroup >/dev/null 2>&1; then
      GROUP_CMD="addgroup --quiet --system $SERVER_GROUP"
   else
      GROUP_CMD="groupadd -r $SERVER_GROUP"
   fi
   
   if $GROUP_CMD 2>&1; then
      echo "..done"
   else
      # Check if group now exists (might have been created despite error message)
      if getent group "$SERVER_GROUP" >/dev/null; then
         echo "..done (already exists)"
      else
         echo "..failed"
         echo "ERROR: Failed to create group $SERVER_GROUP"
         exit 1
      fi
   fi
fi

# 2. create user if not existing
if ! getent passwd $SERVER_USER >/dev/null; then
  echo -n "Adding system user $SERVER_USER.."
  # Try adduser first (Debian/Ubuntu), fall back to useradd (universal)
  if command -v adduser >/dev/null 2>&1; then
     USER_CMD="adduser --quiet --system --ingroup $SERVER_GROUP --no-create-home --disabled-password $SERVER_USER"
  else
     USER_CMD="useradd -r -g $SERVER_GROUP -s /sbin/nologin -d /var/spool/$SERVER_USER $SERVER_USER"
  fi
  
  if $USER_CMD 2>&1; then
     echo "..done"
  else
     # Check if user now exists (might have been created despite error message)
     if getent passwd $SERVER_USER >/dev/null; then
        echo "..done (already exists)"
     else
        echo "..failed"
        echo "ERROR: Failed to create user $SERVER_USER"
        exit 1
     fi
  fi
fi

# 3. adjust passwd entry
usermod -c "$SERVER_NAME" \
        -d $SERVER_HOME   \
        -g $SERVER_GROUP  \
           $SERVER_USER
