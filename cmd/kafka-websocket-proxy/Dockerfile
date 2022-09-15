# syntax=docker/dockerfile:1

# Adapted from https://docs.docker.com/language/golang/build-images/

FROM golang:1.19-alpine3.16 AS build

RUN apk update
RUN apk add git

WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY ./ ./
RUN go build -o /kafka-websocket-proxy cmd/kafka-websocket-proxy/main.go

FROM alpine:3.16

COPY --from=build /kafka-websocket-proxy /kafka-websocket-proxy

EXPOSE 8080

ENTRYPOINT [ "/kafka-websocket-proxy" ]
