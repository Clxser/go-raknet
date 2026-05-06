package raknet_test

import (
	"net"
	"strings"
	"testing"
	"time"

	"github.com/sandertv/go-raknet"
)

func testListener(t *testing.T) (*raknet.Listener, string) {
	t.Helper()
	l, err := raknet.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() {
		_ = l.Close()
	})
	l.PongData([]byte("MCPE;Test Server"))
	return l, l.Addr().String()
}

func TestPing(t *testing.T) {
	const prefix = "MCPE"
	l, addr := testListener(t)
	defer l.Close()

	data, err := raknet.Ping(addr)
	if err != nil {
		t.Fatalf("error pinging %v: %v", addr, err)
	}
	str := string(data)
	if !strings.HasPrefix(str, prefix) {
		t.Fatalf("ping data should have prefix %v, but got %v", prefix, str)
	}
}

func TestPingWithCustomDialer(t *testing.T) {
	const prefix = "MCPE"
	l, addr := testListener(t)
	defer l.Close()

	localDialAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("error resolving local dial address: %v", err)
	}

	dialer := raknet.Dialer{
		UpstreamDialer: &net.Dialer{
			LocalAddr: localDialAddr,
		},
	}

	data, err := dialer.Ping(addr)
	if err != nil {
		t.Fatalf("error pinging %v: %v", addr, err)
	}
	str := string(data)
	if !strings.HasPrefix(str, prefix) {
		t.Fatalf("ping data should have prefix %v, but got %v", prefix, str)
	}
}

func TestDial(t *testing.T) {
	l, addr := testListener(t)
	accepted := acceptOnce(t, l)

	conn, err := raknet.Dial(addr)
	if err != nil {
		t.Fatalf("error connecting to %v: %v", addr, err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("error closing connection: %v", err)
	}
	<-accepted
}

func TestDialWithCustomDialer(t *testing.T) {
	l, addr := testListener(t)
	accepted := acceptOnce(t, l)

	localDialAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("error resolving local dial address: %v", err)
	}

	dialer := raknet.Dialer{
		UpstreamDialer: &net.Dialer{
			LocalAddr: localDialAddr,
		},
	}
	conn, err := dialer.Dial(addr)
	if err != nil {
		t.Fatalf("error connecting to %v: %v", addr, err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("error closing connection: %v", err)
	}
	<-accepted
}

func acceptOnce(t *testing.T, l *raknet.Listener) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := l.Accept()
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		_ = conn.Close()
		_ = l.Close()
	}()
	select {
	case <-done:
		t.Fatal("listener closed before dial")
	case <-time.After(10 * time.Millisecond):
	}
	return done
}
