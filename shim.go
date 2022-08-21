package shim

import (
	"context"
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

var (
	ErrUnalignedWrite = errors.New("write unaligned with websocket frame")
)

type InvalidNetworkError string

func (e InvalidNetworkError) Error() string {
	return "invalid network " + string(e)
}

type UnexpectedMessageTypeError int

func (e UnexpectedMessageTypeError) Error() string {
	return "unexpected websocket message type " + strconv.Itoa(int(e))
}

type Dialer struct{}

func (d Dialer) Dial(network, addr string) (net.Conn, error) {
	return d.DialContext(context.Background(), network, addr)
}

func (d Dialer) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if network != "tcp" {
		return nil, InvalidNetworkError(network)
	}

	u, err := url.Parse(addr)
	if err != nil {
		return nil, errors.Wrap(err, "parse address failed")
	}
	u.Scheme = "wss"
	ws, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		return nil, errors.Wrap(err, "dial websocket failed")
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
func (c *Conn) Write(b []byte) (int, error) {
	written := 0
	for len(b) > 0 {
		if len(b) < sizeBytes {
			return written, ErrUnalignedWrite
		}
		size := parseInt32(b[0], b[1], b[2], b[3])
		if len(b[sizeBytes:]) < int(size) {
			return written, ErrUnalignedWrite
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

// Assumes big endian byte order!
func parseInt32(a, b, c, d byte) int32 {
	var val int32
	val |= int32(a)
	val |= int32(b) << 8
	val |= int32(c) << 16
	val |= int32(d) << 24
	return val
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
