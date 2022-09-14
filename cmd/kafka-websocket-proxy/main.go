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
	"sync"
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
	port   = flag.String("port", "8080", "the port to listen on (default 8080)")
	broker = flag.String("broker", "localhost:8787", "the address of the broker (default localhost:8787)")
	tls    = flag.Bool("tls", false, "whether to use tls for the broker connection (default false)")
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

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-ctx.Done():
					return
				default:
					fmt.Printf("tcp listener failed: %v\n", err)
					cancel()
					return
				}
			}
			defer conn.Close()

			connAddr := conn.RemoteAddr().String()
			fmt.Printf("accepted tcp connection from %s\n", connAddr)

			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := handleClient(ctx, conn, dialer); err != nil {
					fmt.Printf("connection with %s failed: %v\n", connAddr, err)
				} else {
					fmt.Printf("closed tcp connection with %s\n", connAddr)
				}
			}()
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT)

	select {
	case s := <-sig:
		fmt.Printf("received %s, starting graceful shutdown\n", s.String())
		cancel()
	case <-ctx.Done():
		// TCP listener failed and started shutdown on its own
		fmt.Println("tcp listener failed, starting graceful shutdown")
	}

	ln.Close()
	wg.Wait()
}

func handleClient(ctx context.Context, conn net.Conn, dialer proxy.ContextDialer) error {
	ws, err := dialer.DialContext(ctx, "tcp", *broker)
	if err != nil {
		return errors.Wrap(err, "dial websocket failed")
	}
	defer ws.Close()
	fmt.Printf("opened websocket connection with %s\n", ws.RemoteAddr().String())

	g, ctx := errgroup.WithContext(ctx)

	// Pipe data from TCP connection to WebSocket connection
	g.Go(pipeFunc(ctx, conn, ws))
	// Pipe data from WebSocket connection to TCP connection
	g.Go(pipeFunc(ctx, ws, conn))

	g.Go(func() error {
		<-ctx.Done()
		conn.Close()
		ws.Close()
		return nil
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
