
<div align="center">

  <h1>XRootD Monitoring Shoveler</h1>
  
  <p>
    This shoveler gathers UDP monitoring messages from XRootD servers and sends them to a reliable message bus. It supports two operating modes: shoveling mode (minimal processing) and collector mode (full packet parsing and correlation).
  </p>

<!-- Badges -->
  <p>
    <img src="https://img.shields.io/github/actions/workflow/status/opensciencegrid/xrootd-monitoring-shoveler/test.yml?label=Unit%20Testing" alt="Unit Tests" />
    <img src="https://img.shields.io/github/actions/workflow/status/opensciencegrid/xrootd-monitoring-shoveler/golangci-lint.yml?label=Go%20Linting" alt="Linting" />
    <img src="https://img.shields.io/github/actions/workflow/status/opensciencegrid/xrootd-monitoring-shoveler/codeql-analysis.yml?label=CodeQL%20Static%20Analysis" alt="Static Code Analysis" />
    <a href="https://pkg.go.dev/github.com/opensciencegrid/xrootd-monitoring-shoveler">
       <img src="https://pkg.go.dev/badge/github.com/opensciencegrid/xrootd-monitoring-shoveler.svg" alt="Go Reference">
    </a>
    <a href="https://github.com/opensciencegrid/xrootd-monitoring-shoveler/blob/main/LICENSE.txt">
       <img src="https://img.shields.io/github/license/opensciencegrid/xrootd-monitoring-shoveler" alt="license" />
    </a>
  </p>
 
  <h4>
    <a href="https://opensciencegrid.org/docs/data/xrootd/install-shoveler/">Documentation</a>
  <span> · </span>
    <a href="https://github.com/opensciencegrid/xrootd-monitoring-shoveler/issues/">Report Bug</a>
  <span> · </span>
    <a href="https://github.com/opensciencegrid/xrootd-monitoring-shoveler/issues/">Request Feature</a>
  </h4>
</div>

```mermaid
graph LR
  subgraph Site
    subgraph Node 1
    node1[XRootD] -- UDP --> shoveler1{Shoveler};
    end
    subgraph Node 2
    node2[XRootD] -- UDP --> shoveler1{Shoveler};
    end
  end;
  subgraph OSG Operations
  shoveler1 -- TCP/TLS --> C[Message Bus];
  C -- Raw --> D[XRootD Collector];
  D -- Summary --> C;
  C --> E[(Storage)];
  style shoveler1 font-weight:bolder,stroke-width:4px,stroke:#E74C3C,font-size:4em,color:#E74C3C
  end;
```

<!-- Table of Contents -->
# :notebook_with_decorative_cover: Table of Contents

- [:notebook\_with\_decorative\_cover: Table of Contents](#notebook_with_decorative_cover-table-of-contents)
  - [Getting Started](#getting-started)
    - [Requirements](#requirements)
    - [:gear: Installation](#gear-installation)
  - [Configuration](#configuration)
    - [Operating Modes](#operating-modes)
    - [Message Bus Credentials](#message-bus-credentials)
    - [Packet Verification](#packet-verification)
    - [IP Mapping](#ip-mapping)
  - [Running the Shoveler](#running-the-shoveler)
  - [:compass: Design](#compass-design)
    - [Operating Modes](#operating-modes-1)
    - [Queue Design](#queue-design)
  - [:warning: License](#warning-license)
  - [:gem: Acknowledgements](#gem-acknowledgements)

## Getting Started

### Requirements

1. An open UDP port from the XRootD servers, defaults to port 9993.  The port does not need to be open to the public 
   internet, only the XRootD servers.
2. Outgoing network access to connect to the message bus.
3. Disk space for a persistent message queue if the shoveler is disconnected from the message bus.
[Calculations](https://gist.github.com/djw8605/79b3b5a3f5b928f2f50ff469ce57d028) have shown production servers 
   generate <30 MB of data a day.

The shoveler can run on a dedicated server or on a shared server.  The shoveler does not require many resources.
For example, a shoveler serving 12 production XRootD servers can be expected to consume 10-50 MB of ram, 
and require a small fraction of a CPU.

### :gear: Installation

Binaries and packages are provided in the latest Github [releases](https://github.com/opensciencegrid/xrootd-monitoring-shoveler/releases).

## Configuration

The shoveler will read from:

1. Configuration file.
2. Environment Variables
3. Command line arguments.

An example configuration file, [config.yaml](config/config.yaml) is in the repo.  Each variable in the configuration 
file has a corresponding environment variable, listed below.  The environment variables are useful for deployment in 
docker or kubernetes.  By default, the config is stored in `/etc/xrootd-monitoring-shoveler`.

When running as a daemon, environment variables can still be used for configuration. The service will be looking for
them under `/etc/sysconfig/xrootd-monitoring-shoveler`.

### Operating Modes

The shoveler supports two operating modes:

#### Shoveling Mode (Default)

The traditional mode that performs minimal processing:
- Validates packet boundaries and type
- Forwards packets to message bus with minimal overhead
- Preserves current behavior for maximum throughput
- Suitable for high-volume environments

Configure with `mode: shoveling` or leave unset (default).

#### Collector Mode

Advanced mode with full packet parsing and correlation:
- Parses XRootD monitoring packets according to the [XRootD monitoring specification](https://xrootd.web.cern.ch/doc/dev6/xrd_monitoring.htm#_Toc204013498)
- Correlates file open and close events to compute latency and throughput
- Maintains stateful tracking of file operations with TTL-based cleanup
- Emits structured collector records with detailed metrics
- Tracks parsing performance and state management via Prometheus metrics

Configure with `mode: collector` and set state management parameters:

```yaml
mode: collector

state:
  entry_ttl: 300      # Time-to-live for state entries in seconds
  max_entries: 10000  # Maximum state entries (0 for unlimited)
```

See [config-collector.yaml](config/config-collector.yaml) for a complete example.

Environment variables:

* SHOVELER_MODE (shoveling or collector)
* SHOVELER_MQ
* SHOVELER_AMQP_TOKEN_LOCATION
* SHOVELER_AMQP_URL
* SHOVELER_AMQP_EXCHANGE
* SHOVELER_LISTEN_PORT
* SHOVELER_LISTEN_IP
* SHOVELER_VERIFY
* SHOVELER_QUEUE_DIRECTORY
* SHOVELER_STOMP_USER
* SHOVELER_STOMP_PASSWORD
* SHOVELER_STOMP_URL
* SHOVELER_STOMP_TOPIC
* SHOVELER_STOMP_CERT
* SHOVELER_STOMP_CERT_KEY
* SHOVELER_METRICS_PORT
* SHOVELER_METRICS_ENABLE
* SHOVELER_MAP_ALL
* SHOVELER_STATE_ENTRY_TTL (collector mode)
* SHOVELER_STATE_MAX_ENTRIES (collector mode)
* SHOVELER_INPUT_TYPE (udp or message_bus)
* SHOVELER_INPUT_BUFFER_SIZE

### Message Bus Credentials

When running using AMQP as the protocol to connect the shoveler uses a [JWT](https://jwt.io/) to authorize with the message bus.  The token will be issued by an 
automated process, but for now, long lived tokens are issued to sites. 

On the other hand, if STOMP is the selected protocol user and password will need to be provided when configuring the shoveler.

### Packet Verification

If the `verify` option or `SHOVELER_VERIFY` env. var. is set to `true` (the default), the shoveler will perform 
simple verification that the incoming UDP packets conform to XRootD monitoring packets.

### IP Mapping

When the shoveler runs on the same node as the XRootD server, or in the same private network, the IP of the incoming XRootD
packets may report the private IP address rather than the public IP address.  The public ip address is used for reverse
DNS lookup when summarizing the records.  You may map incoming IP addresses to other addresses with the `map` configuration value.

To map all incoming messages to a single IP:

```
map:
  all: <ip address>
```

or the environment variable SHOVELER_MAP_ALL=<ip address>

To map multiple ip addresses, the config file would be:
   
```
map:
   <ip address>: <ip address>
   <ip address>: <ip address>
   
```

## Running the Shoveler

The shoveler is a statically linked binary, distributed as an RPM and uploaded to docker hub and OSG's container hub.
You will need to configure the config.yaml before starting.

Install the RPM from the [latest release](https://github.com/opensciencegrid/xrootd-monitoring-shoveler/releases).  
Start the systemd service with:

    systemctl start xrootd-monitoring-shoveler.service

From Docker, you can start the container from the OSG hub with the following command.

    docker run -v config.yaml:/etc/xrootd-monitoring-shoveler/config.yaml hub.opensciencegrid.org/opensciencegrid/xrootd-monitoring-shoveler

## :compass: Design 

### Operating Modes

The shoveler implements two distinct processing pipelines:

#### Shoveling Mode Pipeline
1. Receive UDP packet
2. Optional: Validate packet header
3. Package packet with metadata (IP, timestamp)
4. Enqueue to message bus
5. Optional: Forward to additional UDP destinations

#### Collector Mode Pipeline
1. Receive UDP packet
2. Parse packet according to XRootD monitoring specification
3. Extract structured fields (file operations, user info, etc.)
4. Correlate with existing state (open/close matching)
5. Calculate metrics (latency, throughput)
6. Emit structured collector record
7. Enqueue to message bus

The collector mode uses a TTL-based state map with automatic cleanup to track file operations across multiple packets. This enables correlation of file open events with their corresponding close events to compute accurate latency and transfer metrics.

### Queue Design

The shoveler receives UDP packets and stores them onto a queue before being sent to the message bus.  100 messages 
are stored in memory.  When the in memory messages reaches over 100, the messages are written to disk under the 
`SHOVELER_QUEUE_DIRECTORY` (env) or `queue_directory` (yaml) configured directories.  A good default is 
`/var/spool/xrootd-monitoring-shoveler/queue`. Note that `/var/run` or `/tmp` should not be used, as these directories
 are not persistent and may be cleaned regularly by tooling such as `systemd-tmpfiles`.
The on-disk queue is persistent across shoveler restarts.

The queue length can be monitored through the prometheus monitoring metric name: `shoveler_queue_size`.

### Metrics

The shoveler exports Prometheus metrics for monitoring. Common metrics include:

**Shoveling Mode:**
- `shoveler_packets_received` - Total packets received
- `shoveler_validations_failed` - Packets that failed validation
- `shoveler_queue_size` - Current queue size
- `shoveler_rabbitmq_reconnects` - MQ reconnection count

**Collector Mode (additional):**
- `shoveler_packets_parsed_ok` - Successfully parsed packets
- `shoveler_parse_errors` - Parse errors by reason
- `shoveler_state_size` - Current state map entries
- `shoveler_ttl_evictions` - State entries evicted due to TTL
- `shoveler_records_emitted` - Collector records emitted
- `shoveler_parse_time_ms` - Packet parsing time histogram
- `shoveler_request_latency_ms` - Request latency histogram

Metrics are available at `http://localhost:8000/metrics` by default (configurable via `metrics.port`).

## :warning: License

Distributed under the [Apache 2.0](https://choosealicense.com/licenses/apache-2.0/) License. See LICENSE.txt for more information.


## :gem: Acknowledgements

This project is supported by the National Science Foundation under Cooperative Agreements [OAC-2030508](https://www.nsf.gov/awardsearch/showAward?AWD_ID=2030508) and [OAC-1836650](https://www.nsf.gov/awardsearch/showAward?AWD_ID=1836650).


