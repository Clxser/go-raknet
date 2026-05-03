package raknet

import (
	"time"
)

// datagramWindow is a queue for incoming datagrams.
type datagramWindow struct {
	lowest, highest uint24
	queue           map[uint24]time.Time
}

// newDatagramWindow returns a new initialised datagram window.
func newDatagramWindow() *datagramWindow {
	return &datagramWindow{queue: make(map[uint24]time.Time)}
}

// add puts an index in the window.
func (win *datagramWindow) add(index uint24) bool {
	if win.seen(index) {
		return false
	}
	if !uint24Less(index, win.highest) {
		win.highest = nextUint24(index)
	}
	win.queue[index] = time.Now()
	return true
}

// seen checks if the index passed is known to the datagramWindow.
func (win *datagramWindow) seen(index uint24) bool {
	if uint24Less(index, win.lowest) {
		return true
	}
	_, ok := win.queue[index]
	return ok
}

// shift attempts to delete as many indices from the queue as possible,
// increasing the lowest index if and when possible.
func (win *datagramWindow) shift() (n int) {
	index := win.lowest
	for index != win.highest {
		if _, ok := win.queue[index]; !ok {
			break
		}
		delete(win.queue, index)
		n++
		index = nextUint24(index)
	}
	win.lowest = index
	return n
}

// missing returns a slice of all indices in the datagram queue that weren't
// set using add while within the window of lowest and highest index. The queue
// is shifted after this call.
func (win *datagramWindow) missing(since time.Duration) (indices []uint24) {
	missing := false
	for index := win.highest; index != win.lowest; {
		index = (index - 1) & uint24Mask
		t, ok := win.queue[index]
		if ok {
			if time.Since(t) >= since {
				// All packets before this one took too long to arrive, so we
				// mark them as missing.
				missing = true
			}
			continue
		}
		if missing {
			indices = append(indices, index)
			win.queue[index] = time.Time{}
		}
	}
	win.shift()
	return indices
}

// size returns the size of the datagramWindow.
func (win *datagramWindow) size() uint24 {
	return uint24Distance(win.lowest, win.highest)
}
