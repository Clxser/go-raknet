package raknet

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sandertv/go-raknet/internal/message"
)

func TestPacketReadRejectsZeroSplitCount(t *testing.T) {
	b := []byte{
		byte(reliabilityReliableOrdered<<5) | splitFlag,
		0, 8, // one byte content, encoded in bits.
		0, 0, 0, // message index.
		0, 0, 0, 0, // order index + order channel.
		0, 0, 0, 0, // invalid split count.
		0, 0, // split ID.
		0, 0, 0, 0, // split index.
		1, // content.
	}
	if _, err := new(packet).read(b); err == nil {
		t.Fatal("expected error for zero split count")
	}
}

func TestPacketReadRejectsSplitIndexOutOfRange(t *testing.T) {
	b := []byte{
		byte(reliabilityReliableOrdered<<5) | splitFlag,
		0, 8, // one byte content, encoded in bits.
		0, 0, 0, // message index.
		0, 0, 0, 0, // order index + order channel.
		0, 0, 0, 1, // split count.
		0, 0, // split ID.
		0, 0, 0, 1, // invalid split index == split count.
		1, // content.
	}
	if _, err := new(packet).read(b); err == nil {
		t.Fatal("expected error for split index out of range")
	}
}

func TestPacketQueueWrapsUint24(t *testing.T) {
	q := newPacketQueue()
	q.lowest = uint24Mask - 1
	q.highest = uint24Mask - 1

	if !q.put(uint24Mask-1, []byte{1}) || !q.put(uint24Mask, []byte{2}) || !q.put(0, []byte{3}) {
		t.Fatal("expected wrapped packet indices to be accepted")
	}
	packets := q.fetch()
	if len(packets) != 3 {
		t.Fatalf("expected 3 packets across wrap, got %v", len(packets))
	}
}

func TestReceiveSplitPacketRejectsZeroCount(t *testing.T) {
	conn := &Conn{handler: dialerConnectionHandler{}, splits: map[uint16][][]byte{}}
	err := conn.receiveSplitPacket(&packet{split: true, splitCount: 0, splitIndex: 0})
	if err == nil {
		t.Fatal("expected error for zero split count")
	}
}

func TestReceiveSplitPacketRejectsSplitCountMismatch(t *testing.T) {
	conn := &Conn{handler: dialerConnectionHandler{}, splits: map[uint16][][]byte{}}
	if err := conn.receiveSplitPacket(&packet{
		split: true, splitCount: 2, splitIndex: 0, splitID: 1, content: []byte{1},
	}); err != nil {
		t.Fatalf("first split packet rejected: %v", err)
	}
	if err := conn.receiveSplitPacket(&packet{
		split: true, splitCount: 3, splitIndex: 1, splitID: 1, content: []byte{2},
	}); err == nil {
		t.Fatal("expected error for split count mismatch")
	}
}

func TestAcknowledgementRejectsMalformedRange(t *testing.T) {
	b := bytes.NewBuffer(nil)
	writeUint16(b, 1)
	b.WriteByte(packetRange)
	writeUint24(b, 5)
	writeUint24(b, 4)
	if err := new(acknowledgement).read(b.Bytes()); !errors.Is(err, errMalformedAcknowledgement) {
		t.Fatalf("expected malformed acknowledgement error, got %v", err)
	}
}

func TestFlushACKsClearsSliceOnClosedWriteError(t *testing.T) {
	conn := &Conn{
		conn:  closedPacketConn{},
		raddr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 19132},
		mtu:   maxMTUSize,
		handler: dialerConnectionHandler{
			l: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
		ackBuf:   bytes.NewBuffer(nil),
		ackSlice: []uint24{1},
	}
	conn.flushACKs()
	if len(conn.ackSlice) != 0 {
		t.Fatalf("ackSlice length = %v, want 0", len(conn.ackSlice))
	}
}

func TestHandleOpenConnectionRequest2DuplicateUsesExistingMTU(t *testing.T) {
	pc := &recordingPacketConn{laddr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 19132}}
	l := &Listener{
		conf:     ListenConfig{DisableCookies: true, ErrorLog: slog.New(slog.NewTextHandler(io.Discard, nil))},
		conn:     pc,
		incoming: make(chan *Conn),
		closed:   make(chan struct{}),
		id:       1,
	}
	h := &listenerConnectionHandler{l: l, cookieSalt: &atomic.Uint64{}, previousSalt: &atomic.Uint64{}}
	addr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 19133}
	l.connections.Store(addrKey(addr), &Conn{mtu: 600})

	request, _ := (&message.OpenConnectionRequest2{
		ServerAddress: resolve(addr),
		MTU:           1200,
		ClientGUID:    -1,
	}).MarshalBinary()
	if err := h.handleOpenConnectionRequest2(request[1:], addr); err != nil {
		t.Fatalf("handle duplicate OCR2: %v", err)
	}
	if len(pc.bufs) != 1 {
		t.Fatalf("expected one reply, got %v", len(pc.bufs))
	}
	reply := &message.OpenConnectionReply2{}
	if err := reply.UnmarshalBinary(pc.bufs[0][1:]); err != nil {
		t.Fatalf("unmarshal reply: %v", err)
	}
	if reply.MTU != 600 {
		t.Fatalf("reply MTU = %v, want existing conn MTU 600", reply.MTU)
	}
}

func TestNextDialerIDAlwaysNegative(t *testing.T) {
	for range 10_000 {
		if id := nextDialerID(); id >= 0 {
			t.Fatalf("nextDialerID returned non-negative value %v", id)
		}
	}
}

type closedPacketConn struct{}

func (closedPacketConn) ReadFrom([]byte) (int, net.Addr, error) { return 0, nil, net.ErrClosed }
func (closedPacketConn) WriteTo([]byte, net.Addr) (int, error)  { return 0, net.ErrClosed }
func (closedPacketConn) Close() error                           { return nil }
func (closedPacketConn) LocalAddr() net.Addr {
	return &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 19131}
}
func (closedPacketConn) SetDeadline(time.Time) error      { return nil }
func (closedPacketConn) SetReadDeadline(time.Time) error  { return nil }
func (closedPacketConn) SetWriteDeadline(time.Time) error { return nil }
