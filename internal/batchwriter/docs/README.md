# Batch Writer Module

## Overview

The batchwriter module provides buffered batch writing with event-driven flushing.

## Design

- **No timers**: Flushing is triggered only when the write channel is drained
- **Event-driven**: Consumer goroutine processes writes as they arrive
- **Batching**: Multiple writes are batched together when the channel drains

## Architecture

```
User Code → Write() → Channel → Consumer Loop → processWrite() → Actual I/O
```

## Usage

```go
bw := batchwriter.New(1024) // buffer size

bw.Write(batchwriter.WriteRequest{
    Data:    data,
    Offset:  offset,
    WriteAt: myWriteFunc,
    Callback: func(n int, err error) {
        // called when write completes
    },
})

bw.Close() // wait for all pending writes
```

## Module Structure

```
batchwriter/
├── api/api.go        # Public interface and implementation
├── internal/         # Private implementation (future extensibility)
├── docs/README.md    # This file
└── batchwriter.go    # Re-export from api
```
