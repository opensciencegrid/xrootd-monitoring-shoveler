XRootD Monitoring Shoveler
==========================

This shoveler gathers UDP messages and sends them to a message bus.
This shoveling is to convert unreliable UDP to reliable message bus.


Configuration
-------------

The shoveler will read from configuration file, environment variables, and finally command line arguments.

Environment variables:

* SHOVELER_AMQP_TOKEN_LOCATION
* SHOVELER_AMQP_URL
* SHOVELER_LISTEN_PORT
* SHOVELER_LISTEN_IP
