package shim

import (
	"context"
	"encoding/binary"
	"net"
	"net/url"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pkg/errors"
)

const (
	sizeBytes = 4
)

type InvalidNetworkError string

func (e InvalidNetworkError) Error() string {
	return "shim: invalid network: expected tcp but got " + string(e)
}

type UnalignedWriteError int

func (e UnalignedWriteError) Error() string {
	return "shim: unaligned write: final byte at index " +
		strconv.Itoa(int(e)) + " is not aligned with websocket frame"
}

type UnexpectedMessageTypeError int

func (e UnexpectedMessageTypeError) Error() string {
	return "shim: invalid websocket message type: expected " +
		strconv.Itoa(int(websocket.BinaryMessage)) + " but got " +
		strconv.Itoa(int(e))
}

type Dialer struct {
	tls bool
}

func NewDialer(tls bool) *Dialer {
	return &Dialer{tls: tls}
}

func (d Dialer) Dial(network, addr string) (net.Conn, error) {
	return d.DialContext(context.Background(), network, addr)
}

func (d Dialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if network != "tcp" {
		return nil, InvalidNetworkError(network)
	}
	u := url.URL{Host: addr}
	if d.tls {
		u.Scheme = "wss"
	} else {
		u.Scheme = "ws"
	}
	ws, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "shim: dial websocket failed")
	}
	return &Conn{ws: ws}, nil
}

// Note: Only Kafka messages can be read or written. This means no TLS
// handshake! However, this shouldn't be a problem since the underlying
// websocket connection will use TLS, and only the traffic between the client
// and the shim will be unencrypted.
type Conn struct {
	ws *websocket.Conn
}

// Potential problem if r is not read completely and read is called again, some
// bytes could be dropped!
// TODO: Needs error translation?
func (c *Conn) Read(b []byte) (int, error) {
	msgType, r, err := c.ws.NextReader()
	if err != nil {
		return 0, err
	}
	if msgType != websocket.BinaryMessage {
		return 0, UnexpectedMessageTypeError(msgType)
	}
	return r.Read(b)
}

// TODO: io.Writer says: "Write must return a non-nil error if it returns n <
// len(p)." It's not clear if this also applies to the Write method of net.Conn.
// We might need to violate this invariant here to match the behavior of TCP,
// which would have no problem with partial (or "unaligned") message writes.
// TODO: Needs error translation?
func (c *Conn) Write(b []byte) (int, error) {
	written := 0
	for len(b) > 0 {
		if len(b) < sizeBytes {
			return written, UnalignedWriteError(written + len(b) - 1)
		}
		size := int32(binary.BigEndian.Uint32(b))
		if len(b[sizeBytes:]) < int(size) {
			return written, UnalignedWriteError(written + len(b) - 1)
		}
		totalSize := sizeBytes + int(size)
		if err := c.ws.WriteMessage(websocket.BinaryMessage, b[:totalSize]); err != nil {
			return written, err
		}
		written += totalSize
		b = b[totalSize:]
	}
	return written, nil
}

func (c *Conn) Close() error {
	return c.ws.Close()
}

func (c *Conn) LocalAddr() net.Addr {
	return c.ws.LocalAddr()
}

func (c *Conn) RemoteAddr() net.Addr {
	return c.ws.RemoteAddr()
}

func (c *Conn) SetDeadline(t time.Time) error {
	// For some reason there is no c.ws.SetDeadline(t)
	return c.ws.UnderlyingConn().SetDeadline(t)
}

func (c *Conn) SetReadDeadline(t time.Time) error {
	// Equivalent to c.ws.UnderlyingConn().SetReadDeadline(t)
	return c.ws.SetReadDeadline(t)
}

func (c *Conn) SetWriteDeadline(t time.Time) error {
	// Equivalent to c.ws.UnderlyingConn().SetWriteDeadline(t)
	return c.ws.SetWriteDeadline(t)
}
