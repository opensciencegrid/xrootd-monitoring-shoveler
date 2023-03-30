#!/bin/bash

# First argument is the location of the private key
privateKey=$1

# Create the new token
./dist/createtoken_darwin_arm64/createtoken -exchange test-shoveler $privateKey > test_token

# Start the xrootd-monitoring-collector
export SHOVELER_AMQP_TOKEN_LOCATION=test_token
export SHOVELER_AMQP_URL="amqps://clever-turkey.rmq.cloudamqp.com/xrd-mon"
export SHOVELER_LISTEN_PORT="9993"
export SHOVELER_LISTEN_IP="127.0.0.1"
export SHOVELER_DEBUG="true"
export SHOVELER_AMQP_EXCHANGE="test-shoveler"
export SHOVELER_VERIFY="false"
./dist/xrootd-monitoring-shoveler_darwin_arm64/xrootd-monitoring-shoveler -d &
shoveler_pid=$!
sleep 1
python3 tests/sendudp.py 127.0.0.1:9993
# Give it time to startup
sleep 5

# Write nonsence that won't work to the token file
echo "notvalidtoken" > test_token
sleep 10
# Run the very simple test program
python3 tests/sendudp.py 127.0.0.1:9993

sleep 10

#python3 tests/sendudp.py 127.0.0.1:9993

# Update the token file, should be detected and a new connection should be made
./dist/createtoken_darwin_arm64/createtoken -exchange test-shoveler $privateKey > test_token

sleep 15

# Run the very simple test program
python3 tests/sendudp.py 127.0.0.1:9993

#sleep 15
sleep 15

kill $shoveler_pid
