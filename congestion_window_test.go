package raknet

import (
	"bytes"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"
)

func TestCongestionWindowTransmissionBandwidth(t *testing.T) {
	win := newCongestionWindow(1000)
	if got := win.transmissionBandwidth(0); got != 1000 {
		t.Fatalf("initial bandwidth = %v, want 1000", got)
	}
	if got := win.transmissionBandwidth(400); got != 600 {
		t.Fatalf("partial bandwidth = %v, want 600", got)
	}
	if got := win.transmissionBandwidth(1200); got != 0 {
		t.Fatalf("full bandwidth = %v, want 0", got)
	}
}

func TestCongestionWindowAckNakAndResend(t *testing.T) {
	win := newCongestionWindow(1000)
	win.onAck(100*time.Millisecond, 0, 1, true)
	if win.cwnd != 2000 {
		t.Fatalf("cwnd after first ACK = %v, want 2000", win.cwnd)
	}
	const additionalVariance = 30 * time.Millisecond
	wantRTO := 2*100*time.Millisecond + 4*100*time.Millisecond + additionalVariance
	if got := win.rto(); got != wantRTO {
		t.Fatalf("rto after first ACK = %v, want %v", got, wantRTO)
	}

	win.onNAK()
	if got, want := win.ssThresh, 1500.0; got != want {
		t.Fatalf("ssThresh after NACK = %v, want %v", got, want)
	}

	win.onAck(100*time.Millisecond, 1, 2, true)
	if win.cwnd <= 2000 {
		t.Fatalf("cwnd after second ACK = %v, want growth", win.cwnd)
	}
	win.onResend(2)
	if got, want := win.cwnd, 1000.0; got != want {
		t.Fatalf("cwnd after resend = %v, want %v", got, want)
	}
	if got, want := win.ssThresh, 1250.0; got != want {
		t.Fatalf("ssThresh after resend = %v, want %v", got, want)
	}
}

func TestConnQueuesReliableDatagramsUntilAck(t *testing.T) {
	conn := newTestConn(428)

	first := testReliablePacket(360)
	second := testReliablePacket(360)
	if err := conn.queueDatagram(first); err != nil {
		t.Fatalf("queue first datagram: %v", err)
	}
	if err := conn.queueDatagram(second); err != nil {
		t.Fatalf("queue second datagram: %v", err)
	}
	if got := conn.conn.(*recordingPacketConn).writes(); got != 1 {
		t.Fatalf("writes before ACK = %v, want 1", got)
	}
	if got := len(conn.sendQueue); got != 1 {
		t.Fatalf("queued datagrams before ACK = %v, want 1", got)
	}
	if got := conn.Pending(); got != 2 {
		t.Fatalf("pending before ACK = %v, want 2", got)
	}

	ackBuf := bytes.NewBuffer(nil)
	(&acknowledgement{packets: []uint24{0}}).write(ackBuf, conn.effectiveMTU())
	if err := conn.handleACK(ackBuf.Bytes()); err != nil {
		t.Fatalf("handle ACK: %v", err)
	}
	if got := conn.conn.(*recordingPacketConn).writes(); got != 2 {
		t.Fatalf("writes after ACK = %v, want 2", got)
	}
	if got := len(conn.sendQueue); got != 0 {
		t.Fatalf("queued datagrams after ACK = %v, want 0", got)
	}
	if got := conn.Pending(); got != 1 {
		t.Fatalf("pending after ACK = %v, want 1", got)
	}
}

func TestConnAckRetransmittedDatagramSkipsRTTUpdate(t *testing.T) {
	conn := newTestConn(428)
	conn.seq = 1
	conn.congestion.estimatedRTT = 100 * time.Millisecond
	conn.congestion.deviationRTT = 100 * time.Millisecond
	conn.congestion.lastRTT = 100 * time.Millisecond
	beforeCWND := conn.congestion.cwnd

	pk := testReliablePacket(100)
	conn.retransmission.unacknowledged[0] = resendRecord{
		pk:            pk,
		timestamp:     time.Now().Add(-10 * time.Millisecond),
		length:        pk.datagramSize(),
		retransmitted: true,
	}
	conn.retransmission.inFlightBytes = pk.datagramSize()

	ackBuf := bytes.NewBuffer(nil)
	(&acknowledgement{packets: []uint24{0}}).write(ackBuf, conn.effectiveMTU())
	if err := conn.handleACK(ackBuf.Bytes()); err != nil {
		t.Fatalf("handle ACK: %v", err)
	}
	if got, want := conn.congestion.estimatedRTT, 100*time.Millisecond; got != want {
		t.Fatalf("estimatedRTT = %v, want %v", got, want)
	}
	if conn.congestion.cwnd <= beforeCWND {
		t.Fatalf("cwnd = %v, want growth above %v", conn.congestion.cwnd, beforeCWND)
	}
}

func TestPacketSizeMatchesWriteLength(t *testing.T) {
	for _, rel := range []reliability{
		reliabilityUnreliable,
		reliabilityUnreliableSequenced,
		reliabilityReliable,
		reliabilityReliableOrdered,
		reliabilityReliableSequenced,
	} {
		pk := &packet{
			reliability:   rel,
			messageIndex:  1,
			sequenceIndex: 2,
			orderIndex:    3,
			content:       []byte{1, 2, 3},
		}
		buf := bytes.NewBuffer(nil)
		pk.write(buf)
		if got, want := pk.size(), buf.Len(); got != want {
			t.Fatalf("size for reliability %v = %v, write length = %v", rel, got, want)
		}
	}
}

func testReliablePacket(size int) *packet {
	return &packet{
		reliability:  reliabilityReliableOrdered,
		messageIndex: 1,
		orderIndex:   1,
		content:      bytes.Repeat([]byte{1}, size),
	}
}

func newTestConn(mtu uint16) *Conn {
	raddr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 19132}
	return &Conn{
		conn:           &recordingPacketConn{laddr: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 19133}},
		raddr:          raddr,
		mtu:            mtu,
		handler:        testConnectionHandler{},
		buf:            bytes.NewBuffer(make([]byte, 0, mtu-28)),
		ackBuf:         bytes.NewBuffer(make([]byte, 0, 128)),
		nackBuf:        bytes.NewBuffer(make([]byte, 0, 64)),
		retransmission: newRecoveryQueue(),
		congestion:     newCongestionWindow(mtu - 28),
	}
}

type recordingPacketConn struct {
	laddr net.Addr
	bufs  [][]byte
}

func (c *recordingPacketConn) ReadFrom([]byte) (int, net.Addr, error) {
	return 0, nil, io.ErrClosedPipe
}

func (c *recordingPacketConn) WriteTo(b []byte, _ net.Addr) (int, error) {
	copied := append([]byte(nil), b...)
	c.bufs = append(c.bufs, copied)
	return len(b), nil
}

func (c *recordingPacketConn) Close() error {
	return nil
}

func (c *recordingPacketConn) LocalAddr() net.Addr {
	return c.laddr
}

func (c *recordingPacketConn) SetDeadline(time.Time) error {
	return nil
}

func (c *recordingPacketConn) SetReadDeadline(time.Time) error {
	return nil
}

func (c *recordingPacketConn) SetWriteDeadline(time.Time) error {
	return nil
}

func (c *recordingPacketConn) writes() int {
	return len(c.bufs)
}

type testConnectionHandler struct{}

func (testConnectionHandler) handle(*Conn, []byte) (bool, error) {
	return false, nil
}

func (testConnectionHandler) limitsEnabled() bool {
	return false
}

func (testConnectionHandler) maxReceiveWindow() uint24 {
	return defaultMaxReceiveWindow
}

func (testConnectionHandler) maxPendingPackets() int {
	return defaultMaxPendingPackets
}

func (testConnectionHandler) maxSplitCount() uint32 {
	return defaultMaxSplitCount
}

func (testConnectionHandler) maxConcurrentSplits() int {
	return defaultMaxConcurrentSplits
}

func (testConnectionHandler) close(*Conn) {}

func (testConnectionHandler) log() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
