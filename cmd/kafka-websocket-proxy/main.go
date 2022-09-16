package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/maxwellpeterson/kafka-websocket-shim/pkg/shim"
	"github.com/pkg/errors"
	"golang.org/x/net/proxy"
	"golang.org/x/sync/errgroup"
)

const (
	bufSize = 4096
)

var (
	port   = flag.String("port", "8080", "the port to listen on")
	broker = flag.String("broker", "localhost:8787", "the address of the broker")
	tls    = flag.Bool("tls", false, "use tls for the broker connection")
)

func main() {
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	dialer := shim.NewDialer(shim.DialerConfig{TLS: *tls})

	ln, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatal(errors.Wrap(err, "start tcp listener failed"))
	}
	fmt.Printf("listening on port %s\n", *port)

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-ctx.Done():
					return nil
				default:
					// Returning error cancels context and triggers shutdown
					return errors.Wrap(err, "tcp listener failed")
				}
			}

			connAddr := conn.RemoteAddr().String()
			fmt.Printf("accepted tcp connection from %s\n", connAddr)

			g.Go(func() error {
				if err := handleClient(ctx, conn, dialer); err != nil {
					fmt.Printf("connection with %s failed: %v\n", connAddr, err)
				} else {
					fmt.Printf("closed tcp connection with %s\n", connAddr)
				}
				// Individual connections can fail without triggering shutdown
				return nil
			})
		}
	})

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT)

	select {
	case s := <-sig:
		fmt.Printf("received %s, starting graceful shutdown\n", s.String())
		cancel()
	case <-ctx.Done():
		// TCP listener failed and triggered shutdown on its own
	}

	if err := ln.Close(); err != nil {
		log.Fatal(errors.Wrap(err, "close tcp listener failed"))
	}

	if err := g.Wait(); err != nil {
		log.Fatal(err)
	}
}

func handleClient(ctx context.Context, conn net.Conn, dialer proxy.ContextDialer) error {
	ws, err := dialer.DialContext(ctx, "tcp", *broker)
	if err != nil {
		return errors.Wrap(err, "dial websocket failed")
	}
	fmt.Printf("opened websocket connection with %s\n", ws.RemoteAddr().String())

	g, ctx := errgroup.WithContext(ctx)
	// Pipe data from TCP connection to WebSocket connection
	g.Go(pipeFunc(ctx, conn, ws))
	g.Go(func() error {
		<-ctx.Done()
		return conn.Close()
	})
	// Pipe data from WebSocket connection to TCP connection
	g.Go(pipeFunc(ctx, ws, conn))
	g.Go(func() error {
		<-ctx.Done()
		return ws.Close()
	})

	if err := g.Wait(); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

func pipeFunc(ctx context.Context, src net.Conn, dst net.Conn) func() error {
	return func() error {
		buf := make([]byte, bufSize)
		for {
			if _, err := pipe(src, dst, buf); err != nil {
				select {
				case <-ctx.Done():
					return nil
				default:
					return err
				}
			}
		}
	}
}

func pipe(src net.Conn, dst net.Conn, buf []byte) (int, error) {
	n, err := src.Read(buf)
	if err != nil {
		return 0, err
	}
	n, err = dst.Write(buf[:n])
	if err != nil {
		return n, err
	}
	return n, nil
}
