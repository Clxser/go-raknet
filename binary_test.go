package raknet

import (
	"bytes"
	"testing"
)

func Test_uint24(t *testing.T) {
	b := bytes.NewBuffer(nil)
	writeUint24(b, 123456)
	val, err := readUint24(b)
	if err != nil {
		t.Fatalf("error reading uint24: %v", err)
	}
	if val != 123456 {
		t.Fatal("read uint24 was not equal to 123456")
	}
}

func TestUint24IncWraps(t *testing.T) {
	u := uint24(uint24Mask)
	if old := u.Inc(); old != uint24Mask {
		t.Fatalf("expected old value %v, got %v", uint24Mask, old)
	}
	if u != 0 {
		t.Fatalf("expected uint24 to wrap to 0, got %v", u)
	}
}

func TestUint24WindowHelpersWrap(t *testing.T) {
	if !uint24Less(uint24Mask, 0) {
		t.Fatal("expected max uint24 to compare before 0 across wrap")
	}
	if uint24Distance(uint24Mask-1, 1) != 3 {
		t.Fatalf("unexpected wrapped distance: %v", uint24Distance(uint24Mask-1, 1))
	}
}
