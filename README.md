# Kafka WebSocket Shim

**Note:** This is a child project of [`kafka-worker`](https://github.com/maxwellpeterson/kafka-worker)

A shim layer that enables existing Kafka clients to connect to a broker implementation running on Cloudflare Workers with minimal configuration changes.

This project includes a Go package that provides a `net.Conn` implementation that frames Kafka protocol messages into WebSocket messages, plus a `proxy.Dialer` and `proxy.ContextDialer` implementation for creating these connections. It also includes a standalone TCP proxy that provides the same functionality, and can be used with Kafka clients written in other languages.

## Installation

### Go Package

TODO

### TCP Proxy (Go)

TODO

### TCP Proxy (Docker)

TODO

## Map

![kafka worker map](map.png)
