package shim

import (
	"context"
	"encoding/binary"
	"log"
	"net"
	"net/http"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

var (
	msg1 = MakeMsg(100, 'a')
	msg2 = MakeMsg(75, 'b')
	msg3 = MakeMsg(125, 'c')
	msgs = [][]byte{msg1, msg2, msg3}
)

type StopFunc func()

func (f StopFunc) Stop() {
	f()
}

func StartServer(addr string, handler func(*websocket.Conn) error) StopFunc {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(errors.Wrap(err, "server: listen failed"))
	}
	upgrader := websocket.Upgrader{}
	s := http.Server{
		Addr: addr,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			c, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				log.Fatal(errors.Wrap(err, "server: connection upgrade failed"))
			}
			defer c.Close()
			if err = handler(c); err != nil {
				log.Fatal(errors.Wrap(err, "server: handler failed"))
			}
		}),
	}
	go func() {
		if err := s.Serve(l); err != nil && err != http.ErrServerClosed {
			log.Fatal(errors.Wrap(err, "server: serve failed"))
		}
	}()
	return func() {
		if err = s.Shutdown(context.Background()); err != nil {
			log.Fatal(errors.Wrap(err, "server: shutdown failed"))
		}
	}
}

func MakeMsg(length int32, fill byte) []byte {
	msg := make([]byte, sizeBytes+length)
	binary.BigEndian.PutUint32(msg, uint32(length))
	for i := range msg[sizeBytes:] {
		msg[sizeBytes+i] = fill
	}
	return msg
}

func TestReadOne(t *testing.T) {
	addr := "localhost:8080"
	handler := func(c *websocket.Conn) error {
		return c.WriteMessage(websocket.BinaryMessage, msg1)
	}
	defer StartServer(addr, handler).Stop()

	d := NewDialer(false)
	c, err := d.Dial("tcp", addr)
	assert.Nil(t, err)
	defer c.Close()

	buf := make([]byte, 150)
	n, err := c.Read(buf)
	assert.Nil(t, err)
	assert.Equal(t, len(msg1), n, "bytes read matches message length")
	assert.Equal(t, msg1, buf[:n], "buffer matches message")
}

func TestReadMany(t *testing.T) {
	addr := "localhost:8081"
	handler := func(c *websocket.Conn) error {
		for _, msg := range msgs {
			if err := c.WriteMessage(websocket.BinaryMessage, msg); err != nil {
				return err
			}
		}
		return nil
	}
	defer StartServer(addr, handler).Stop()

	d := NewDialer(false)
	c, err := d.Dial("tcp", addr)
	assert.Nil(t, err)
	defer c.Close()

	buf := make([]byte, 150)
	for _, msg := range msgs {
		n, err := c.Read(buf)
		assert.Nil(t, err)
		assert.Equal(t, len(msg), n, "bytes read matches message length")
		assert.Equal(t, msg, buf[:n], "buffer matches message")
	}
}

func ReadUnexpectedMessageType(t *testing.T) {
	addr := "localhost:8082"
	handler := func(c *websocket.Conn) error {
		return c.WriteMessage(websocket.TextMessage, []byte("hello"))
	}
	defer StartServer(addr, handler).Stop()

	d := NewDialer(false)
	c, err := d.Dial("tcp", addr)
	assert.Nil(t, err)
	defer c.Close()

	buf := make([]byte, 150)
	n, err := c.Read(buf)
	assert.ErrorIs(t, err, UnexpectedMessageTypeError(websocket.TextMessage))
	assert.Equal(t, 0, n)
}

func TestWriteOne(t *testing.T) {
	addr := "localhost:8083"
	handler := func(c *websocket.Conn) error {
		mt, p, err := c.ReadMessage()
		if err != nil {
			return err
		}
		assert.Equal(t, websocket.BinaryMessage, mt, "websocket message type is binary")
		assert.Equal(t, msg1, p, "buffer matches message")
		return nil
	}
	defer StartServer(addr, handler).Stop()

	d := NewDialer(false)
	c, err := d.Dial("tcp", addr)
	assert.Nil(t, err)
	defer c.Close()

	n, err := c.Write(msg1)
	assert.Nil(t, err)
	assert.Equal(t, len(msg1), n)
}

func TestWriteMany(t *testing.T) {
	addr := "localhost:8084"
	handler := func(c *websocket.Conn) error {
		for _, msg := range msgs {
			mt, p, err := c.ReadMessage()
			if err != nil {
				return err
			}
			assert.Equal(t, websocket.BinaryMessage, mt, "websocket message type is binary")
			assert.Equal(t, msg, p, "buffer matches message")
		}
		return nil
	}
	defer StartServer(addr, handler).Stop()

	d := NewDialer(false)
	c, err := d.Dial("tcp", addr)
	assert.Nil(t, err)
	defer c.Close()

	for _, msg := range msgs {
		n, err := c.Write(msg)
		assert.Nil(t, err)
		assert.Equal(t, len(msg), n)
	}
}

func TestWriteUnaligned(t *testing.T) {
	addr := "localhost:8085"
	handler := func(c *websocket.Conn) error {
		return nil
	}
	defer StartServer(addr, handler).Stop()

	d := NewDialer(false)
	c, err := d.Dial("tcp", addr)
	assert.Nil(t, err)
	defer c.Close()

	msgTrunc := msg1[:len(msg1)/2]
	n, err := c.Write(msgTrunc)
	assert.ErrorIs(t, err, UnalignedWriteError(len(msgTrunc)-1))
	assert.Equal(t, 0, n)
}
