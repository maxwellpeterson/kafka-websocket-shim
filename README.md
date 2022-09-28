# Kafka WebSocket Shim

**Note:** This is a child project of [`kafka-worker`](https://github.com/maxwellpeterson/kafka-worker)

A shim layer that enables existing Kafka clients to connect to a broker implementation running on Cloudflare Workers with minimal configuration changes. As with the parent project, this is a simple proof of concept and not intended to be used for anything serious.

## Modes of Operation

### Go Package

For Kafka clients written in Go, this project provides a Go package with a `net.Conn` implementation that frames Kafka protocol messages into WebSocket messages, plus a `proxy.Dialer` and `proxy.ContextDialer` implementation that creates these connections.

See [`kafka-worker-demo`](https://github.com/maxwellpeterson/kafka-worker-demo) for an example that uses this package with the [`franz-go`](https://github.com/twmb/franz-go) client.

### TCP Proxy

For Kafka clients written in other languages, this project provides a standalone TCP proxy that frames Kafka protocol messages into WebSocket messages and forwards them to the broker implementation.

See [`kafka-worker-demo`](https://github.com/maxwellpeterson/kafka-worker-demo) for an example that uses the proxy with the [`kafka-python`](https://github.com/dpkp/kafka-python) client.

## How It Works

All Kafka protocol messages use the following grammar (defined in the [Kafka Protocol Guide](https://kafka.apache.org/protocol.html#protocol_common)):

```
RequestOrResponse => Size (RequestMessage | ResponseMessage)
  Size => int32
```

To convert a continuous stream of protocol messages into discrete WebSocket messages, all we need to do is read the size of the first protocol message as a 32-bit integer `n`, read the next `n` bytes of the stream, wrap these `n + 4` bytes in a WebSocket message, and repeat the same process for the remainder of the stream. It's super simple.

## Quick Start

### Go Package

```shell
go get github.com/maxwellpeterson/kafka-websocket-shim/pkg/shim
```

### TCP Proxy (From Source)

This method requires a local installation of Go.

```shell
git clone https://github.com/maxwellpeterson/kafka-websocket-shim.git
cd kafka-websocket-shim
go run cmd/kafka-websocket-proxy/main.go
```

### TCP Proxy (`go install`)

This method requires a local installation of Go, and assumes that `GOPATH/bin` is part of `PATH`.

```shell
go install github.com/maxwellpeterson/kafka-websocket-shim/cmd/kafka-websocket-proxy
kafka-websocket-proxy
```

### TCP Proxy (Docker)

This method requires a local installation of Docker.

```shell
docker run --rm --publish 8080:8080 ghcr.io/maxwellpeterson/kafka-websocket-proxy:main -broker=mybroker.workers.dev:443 -tls
```

To connect to a local instance of `kafka-worker`, it is recommended to run `kafka-worker` in a container and create a shared network for the proxy and broker. Using Docker Compose makes this setup easy, see [`kafka-worker-demo`](https://github.com/maxwellpeterson/kafka-worker-demo) for examples.

## Map

![kafka worker map](map.png)
