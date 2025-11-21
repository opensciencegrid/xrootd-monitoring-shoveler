# Collector Refactoring Summary

## Overview
Refactored the collector main to eliminate code duplication across three input types (File, UDP, RabbitMQ) by extracting common packet processing logic into shared functions.

## Changes Made

### 1. Created Shared Packet Processing Logic

#### New Function: `handleParsedPacket()`
Extracted the common packet processing logic that was duplicated across all three input modes:
- Debug logging of packet details (token info, auth info, user info)
- GStream packet handling and emission
- Regular packet correlation through the correlator
- Record emission with latency tracking

**Benefits:**
- Single source of truth for packet processing
- Consistent behavior across all input types
- Easier to maintain and update

#### New Function: `processPackets()`
Common loop for processing packets from a `PacketSource`:
- Packet parsing with metrics
- Error handling
- Remote address assignment
- Delegation to `handleParsedPacket()`

### 2. Enhanced Input Package

#### Updated `UDPListener` in `input/input.go`
Added support for tracking remote addresses:
- New `PacketWithAddr` struct containing packet data and remote address
- New `PacketsWithAddr()` channel returning packets with their source addresses
- Both `Packets()` and `PacketsWithAddr()` channels maintained for compatibility

**Before:**
```go
type UDPListener struct {
    packets    chan []byte
    // ...
}
```

**After:**
```go
type PacketWithAddr struct {
    Data       []byte
    RemoteAddr string
}

type UDPListener struct {
    packets         chan []byte
    packetsWithAddr chan PacketWithAddr
    // ...
}
```

### 3. Simplified Input Mode Functions

#### File Input (`runCollectorModeFile`)
**Before:** 120+ lines of duplicated parsing and processing logic
**After:** 30 lines - creates FileReader and calls `processPackets()`

**Reduction:** ~75% fewer lines

#### UDP Input (`runCollectorModeUDP`)
**Before:** 140+ lines of duplicated parsing and processing logic
**After:** 50 lines - creates UDPListener and processes packets with remote addresses

**Reduction:** ~64% fewer lines

#### RabbitMQ Input (`runCollectorModeRabbitMQ`)
**Before:** 160+ lines of duplicated parsing and processing logic
**After:** 85 lines - uses existing RabbitMQReader with remote address synchronization

**Reduction:** ~47% fewer lines

### 4. Fixed Queue Tests
Updated `queue_test.go` to work with the new `Dequeue()` return type (`*MessageStruct` instead of `[]byte`):
- `TestQueueInsert`: Fixed to access `msg.Message` field
- `TestQueueEmptyDequeue`: Fixed to access `msg.Message` field  
- `TestQueueLotsEntries`: Fixed to access `msg.Message` field

## Metrics

### Lines of Code
- **Before:** 607 lines in `cmd/collector/main.go`
- **After:** 434 lines in `cmd/collector/main.go`
- **Reduction:** 173 lines (28% reduction)

### Code Duplication
- **Before:** ~400 lines of duplicated packet processing logic across 3 functions
- **After:** ~130 lines of shared logic in 2 functions
- **Eliminated:** ~270 lines of duplication

### Test Status
- ✅ All existing tests pass
- ✅ Parser tests: PASS
- ✅ Collector tests: PASS
- ✅ Input tests: PASS
- ✅ Queue tests: PASS (updated)
- ✅ Build successful for both collector and shoveler

## Architecture Improvements

### Before
```
runCollectorModeFile()
├── Setup FileReader
├── Parse packet (duplicated)
├── Set remote address (duplicated)
├── Debug logging (duplicated)
├── Handle GStream (duplicated)
└── Correlate and emit (duplicated)

runCollectorModeUDP()
├── Setup UDP listener
├── Parse packet (duplicated)
├── Set remote address (duplicated)
├── Debug logging (duplicated)
├── Handle GStream (duplicated)
└── Correlate and emit (duplicated)

runCollectorModeRabbitMQ()
├── Setup RabbitMQ reader
├── Parse packet (duplicated)
├── Set remote address (duplicated)
├── Debug logging (duplicated)
├── Handle GStream (duplicated)
└── Correlate and emit (duplicated)
```

### After
```
handleParsedPacket() [SHARED]
├── Debug logging
├── Handle GStream
└── Correlate and emit

processPackets() [SHARED]
├── Parse packet
├── Set remote address
└── Call handleParsedPacket()

runCollectorModeFile()
├── Setup FileReader
└── Call processPackets()

runCollectorModeUDP()
├── Setup UDPListener
└── Process with remote addresses

runCollectorModeRabbitMQ()
├── Setup RabbitMQ reader
└── Process with remote addresses
```

## Benefits

1. **Maintainability:** Changes to packet processing logic only need to be made once
2. **Consistency:** All input types behave identically
3. **Testability:** Shared functions can be tested independently
4. **Readability:** Input-specific code is now focused on just the input mechanism
5. **Extensibility:** Adding new input types is now much easier

## Backward Compatibility

- ✅ All existing functionality preserved
- ✅ Configuration compatibility maintained
- ✅ Metrics and logging unchanged
- ✅ Output format unchanged
- ✅ All three input modes (File, UDP, RabbitMQ) work as before

## Future Improvements

Potential next steps:
1. Create a unified `PacketSourceWithAddr` interface that all inputs could implement
2. Further extract correlator setup into a shared function
3. Consider adding more input types (e.g., Kafka, HTTP POST)
4. Add integration tests for the refactored code
