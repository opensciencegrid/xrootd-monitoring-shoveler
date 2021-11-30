XRootD Monitoring Shoveler
==========================

This shoveler gathers UDP messages and sends them to a message bus.
This shoveling is used to convert unreliable UDP to reliable message bus.


Installation
------------

Binaries and packages are provided in the latest Github [releases](/opensciencegrid/xrootd-monitoring-shoveler/releases).

Configuration
-------------

The shoveler will read from:

1. Configuration file.
2. Environment Variables
3. Command line arguments.

An example configuration file, [config.yaml](config/config.yaml) is in the repo.  Each variable in the configuration file has a corresponding environment variable, listed below.  The environment variables are useful for deployment in docker or kubernetes.  By default, the config is stored in `/etc/xrootd-monitoring-shoveler`

Environment variables:

* SHOVELER_AMQP_TOKEN_LOCATION
* SHOVELER_AMQP_URL
* SHOVELER_AMQP_EXCHANGE
* SHOVELER_LISTEN_PORT
* SHOVELER_LISTEN_IP
* SHOVELER_VERIFY

Running the Shoveler
--------------------

The shoveler is a statically linked binary, distributed as an RPM and uploaded to docker hub and OSG's container hub. You will need to configure the config.yaml before starting

Install the RPM from the [latest release](/opensciencegrid/xrootd-monitoring-shoveler/releases).  Start the systemd service with:

    systemctl start xrootd-monitoring-shoveler.service

From Docker, you can start the container from the OSG hub with the following command.

    docker run -v config.yaml:/etc/xrootd-monitoring-shoveler/config.yaml hub.opensciencegrid.org/opensciencegrid/xrootd-monitoring-shoveler


Packet Verification
-------------------

If the `verify` option or `SHOVELER_VERIFY` env. var. is set to `true` (the default), the shoveler will perform simple verification that the incoming UDP packets conform to XRootD monitoring packets.