package shim

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"net/url"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pkg/errors"
)

const (
	int32Size = 4
)

type InvalidNetworkError string

func (e InvalidNetworkError) Error() string {
	return fmt.Sprintf("shim: invalid network: expected tcp but got %s", string(e))
}

type InvalidMessageTypeError int

func (e InvalidMessageTypeError) Error() string {
	return fmt.Sprintf("shim: invalid websocket message type: expected %d but got %d",
		websocket.BinaryMessage, e)
}

// Implements proxy.Dialer and proxy.ContextDialer
type Dialer struct {
	tls bool
}

type DialerConfig struct {
	TLS bool
}

func NewDialer(cfg DialerConfig) *Dialer {
	return &Dialer{tls: cfg.TLS}
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

// Implements net.Conn
//
// Note: Only Kafka protocol messages can be read or written. This means no TLS
// handshake! This isn't a serious problem since the underlying WebSocket
// connection can provide TLS on its own
type Conn struct {
	ws   *websocket.Conn
	rBuf []byte
	wBuf []byte
}

func (c *Conn) Read(b []byte) (int, error) {
	if len(c.rBuf) > 0 {
		// If we've buffered the remainder of a WebSocket message that was
		// partially read, read from this buffer first. We don't make another
		// read call to the underlying WebSocket until this buffer is empty,
		// meaning the previous message has been fully read
		n := copy(b, c.rBuf)
		c.rBuf = c.rBuf[n:]
		return n, nil
	}
	msgType, bytes, err := c.ws.ReadMessage()
	if err != nil {
		return 0, errors.Wrap(err, "shim: read websocket message failed")
	}
	if msgType != websocket.BinaryMessage {
		return 0, InvalidMessageTypeError(msgType)
	}
	n := copy(b, bytes)
	c.rBuf = bytes[n:]
	return n, nil
}

func (c *Conn) Write(b []byte) (int, error) {
	written := -len(c.wBuf)
	c.wBuf = append(c.wBuf, b...)
	for len(c.wBuf) > 0 {
		if len(c.wBuf) < int32Size {
			return len(b), nil
		}
		size := int32(binary.BigEndian.Uint32(c.wBuf))
		if len(c.wBuf[int32Size:]) < int(size) {
			return len(b), nil
		}
		totalSize := int32Size + int(size)
		// For now, we send each Kafka protocol message in its own WebSocket
		// message, even if multiple protocol messages are included in the same
		// write call. We could optimize this my by allowing multiple protocol
		// messages to share the same WebSocket message, but we would also need
		// to update broker implementation (which assumes a one-to-one mapping)
		//
		// Note that we also include the original Kafka protocol message size
		// header in the WebSocket message, even though it is redundant since
		// the WebSocket protocol provides message framing for us. We include
		// the size header anyway to match the Kafka protocol spec as closely as
		// possible, knowing that we should be able to ditch the shim and use
		// TCP directly in the future. For now, we want to avoid any protocol
		// modifications that are specific to WebSocket usage
		if err := c.ws.WriteMessage(websocket.BinaryMessage, c.wBuf[:totalSize]); err != nil {
			return max(written, 0), errors.Wrap(err, "shim: write websocket message failed")
		}
		written += totalSize
		c.wBuf = c.wBuf[totalSize:]
	}
	return max(written, 0), nil
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

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
