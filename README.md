# Kafka WebSocket Shim

**Note:** This is a child project of [`kafka-worker`](https://github.com/maxwellpeterson/kafka-worker)

A network shim that enables existing Kafka clients to connect to a broker implementation running on Cloudflare Workers with minimal configuration changes.

For now, this project includes a Go package that provides a `net.Conn` implementation that frames Kafka protocol messages into WebSocket messages. Eventually, I would like to write a standalone TCP proxy that can do the same thing for Kafka clients written in other languages.

## Map

![kafka worker map](map.png)
