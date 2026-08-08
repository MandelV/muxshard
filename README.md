# muxshard

A [yamux](https://github.com/hashicorp/yamux)-style stream multiplexer for Go — with a twist: instead of carrying every logical stream over a single TCP connection, a `muxshard` session is sharded across **several TCP connections at once** ("partitions"). Which partition carries a given stream is decided deterministically by hashing the stream ID, so a stream's frames always land on the same partition for the life of the connection, no matter which side is sending.

```
Client                          Server
┌─────────┐   partition 0   ┌─────────┐
│         │ ─────────────── │         │
│ Session │   partition 1   │ Session │
│         │ ─────────────── │         │
│         │   partition N   │         │
└─────────┘ ─────────────── └─────────┘
     │                            │
   Stream(s)  ◄── multiplexed ──►  Stream(s)
```

The goal is to push more than one TCP connection's worth of throughput between two hosts without giving up yamux's ergonomics: `OpenStream()` / `AcceptStream()` return regular `net.Conn`-like objects, and everything else (framing, backpressure, session bookkeeping) is handled underneath.

## Status

This is a personal project, built and stress-tested (including through a real SOCKS5 proxy tunneling a live speedtest and video streaming) but not hardened for production use. See [Known limitations](#known-limitations) below.

## Install

```sh
go get muxshard
```

(Go 1.26+, module path `muxshard`.)

## Usage

```go
// Server side: accept sessions, then streams within each session.
ln, _ := net.Listen("tcp", ":9000")
server := muxshard.NewServer(ln)
go server.Serve()

for {
    session := server.AcceptSession()
    go func() {
        for {
            stream := session.AcceptStream()
            go handle(stream) // stream implements net.Conn
        }
    }()
}
```

```go
// Client side: dial N partitions to the server, get a ready-to-use session.
client, err := muxshard.NewClient("server:9000", 16) // 16 TCP connections
if err != nil {
    log.Fatal(err)
}

stream, err := client.OpenStream()
// stream.Read / stream.Write / stream.Close, like any net.Conn
```

## How it works

- **Session**: one logical connection between a client and a server, made of one or more **partitions** (raw TCP connections). A session is identified by a `SessionID`, exchanged during the handshake so both peers agree on it.
- **Partition**: a plain TCP connection. Each has its own goroutine reading frames off the wire (`RecvLoop`) and one serializing writes onto it (`SendLoop`).
- **Stream**: a logical, ordered byte stream multiplexed over the session's partitions. Every `Stream.Write` picks its partition via a deterministic hash of `(session seed, stream ID, partition count)` — same inputs, same partition, every time, on both sides (the seed is derived from `SessionID`, so client and server agree without needing to coordinate it separately).
- Demuxing on receive is purely by stream ID, not by which partition a frame arrived on — so even if the two peers' routing decisions land on different physical partitions (e.g. while partition count differs momentarily), correctness doesn't depend on them matching.

## Sandbox CLI

`cmd/muxshard` is a small demo binary exercising the library end to end:

```sh
go run ./cmd/muxshard -mode=server -addr=127.0.0.1:9000
go run ./cmd/muxshard -mode=client -addr=127.0.0.1:9000 -partitions=16
```

## Example: SOCKS5 proxy

`example/` wires `muxshard` up to [`things-go/go-socks5`](https://github.com/things-go/go-socks5) to tunnel a local SOCKS5 proxy over a multi-partition session — a practical way to see the throughput benefit:

```sh
go run ./example
# then point a browser or curl at 127.0.0.1:9001 as a SOCKS5 proxy
```

## Known limitations / todo list

- **No client-side reconciliation loop**: if a partition drops mid-session, `Client.PartitionDown` reports it, but nothing redials to bring the partition count back to the original target.
- **No dynamic resizing on the server**: partitions are only ever added or removed wholesale (session teardown), not safely churned up and down mid-session — `Session.RemovePartition` shifts partition indices, which would remap in-flight streams if partitions kept churning.
- **No keepalive**: `Ping`/`Pong` frames are answered if received, but nothing sends a `Ping` proactively — an idle, half-dead partition (peer gone, no RST reached us) won't be noticed until an actual write on it fails.
- **`GoAway` isn't a real shutdown signal**: it's parsed and stops that partition's read loop, but nothing sends one on `Close`, and receiving one doesn't stop the session from accepting new streams or reject further `OpenStream` calls.
- **No proper half-close**: a `Fin`/`RST` fully closes the stream's local read side, with no way to keep reading while the peer has only stopped writing (no TCP-style `CloseWrite`).
- **No flow control**: unlike yamux's per-stream receive window, delivery is a synchronous, unbuffered handoff (`net.Pipe`) — a slow reader on one stream can block the `RecvLoop` of the partition it shares with other streams (head-of-line blocking), instead of just backing off that one stream.
- General network fault-tolerance (timeouts, retries, partial-write recovery beyond what's listed above) hasn't been exercised much past the load tests in [Status](#status).

## License

MIT — see [LICENSE](LICENSE).
