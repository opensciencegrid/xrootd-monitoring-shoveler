#!/bin/bash

# First argument is the location of the private key
privateKey=$1

# Create the new token
./bin/createtoken $privateKey > test_token

# Start the xrootd-monitoring-collector
export SHOVELER_AMQP_TOKEN_LOCATION=test_token
export SHOVELER_AMQP_URL="amqps://clever-turkey.rmq.cloudamqp.com/xrd-mon"
export SHOVELER_LISTEN_PORT="9993"
export SHOVELER_LISTEN_IP="127.0.0.1"
./bin/shoveler -d &
shoveler_pid=$!

# Give it time to startup
sleep 5

# Write nonsence that won't work to the token file
echo "notvalidtoken" > test_token
sleep 10

# Update the token file, should be detected and a new connection should be made
./bin/createtoken $privateKey > test_token

sleep 15

# Run the very simple test program
python3 tests/sendudp.py 127.0.0.1:9993

sleep 5

kill $shoveler_pid

