# Build Your Own Redis

This is my implementation of the [**Build Your Own Redis**](https://codecrafters.io/challenges/redis) challenge by Codecrafters.io, built using Go.

### Features

- Basic commands: `PING`, `ECHO`, `SET`, `GET` (with expiry)
- Lists: `LPUSH`, `RPUSH`, `LRANGE`, `LPOP`, blocking ops
- Streams: `XADD`, `XRANGE`, `XREAD`, with blocking reads
- Transactions: `MULTI`, `EXEC`, `DISCARD`, `INCR`
- Replication: master–replica sync, offsets, etc.

### Run Locally

To build and run:

```bash
go build -o app
./app --port <port_number>
