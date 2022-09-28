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
	"time"

	"github.com/maxwellpeterson/kafka-websocket-shim/pkg/shim"
	"github.com/pkg/errors"
	"golang.org/x/net/proxy"
	"golang.org/x/sync/errgroup"
)

const (
	pipeBufSize       = 4096
	dialBrokerRetries = 5
	dialBrokerWait    = 200 * time.Millisecond
	dialBrokerBackoff = 2
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
	ws, err := dialBroker(ctx, dialer)
	if err != nil {
		defer conn.Close()
		return errors.Wrap(err, "dial broker failed")
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

// Open a WebSocket connection with the broker, using exponential backoff if the
// connection fails. When running the broker in local mode using Docker Compose,
// the broker takes 1-2 seconds to become ready after the container is created,
// and this backoff gives it plenty of time to become ready
func dialBroker(ctx context.Context, dialer proxy.ContextDialer) (net.Conn, error) {
	var dialErr error
	wait := dialBrokerWait
	for i := 0; i < dialBrokerRetries; i++ {
		if ws, err := dialer.DialContext(ctx, "tcp", *broker); err != nil {
			if i < dialBrokerRetries-1 {
				// Don't sleep on the final iteration, because
				// dialer.DialContext won't be called again
				time.Sleep(wait)
				wait *= dialBrokerBackoff
			}
			dialErr = err
		} else {
			return ws, nil
		}
	}
	return nil, dialErr
}

func pipeFunc(ctx context.Context, src net.Conn, dst net.Conn) func() error {
	return func() error {
		buf := make([]byte, pipeBufSize)
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
