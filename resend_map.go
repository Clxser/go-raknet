package raknet

import (
	"time"
)

// resendMap is a map of packets, used to recover datagrams if the other end of
// the connection ended up not having them.
type resendMap struct {
	unacknowledged map[uint24]resendRecord
	delays         []delayRecord
	inFlightBytes  int
}

type delayRecord struct {
	timestamp time.Time
	delay     time.Duration
}

// resendRecord represents a single packet with a timestamp from when it was
// initially sent. It may be either acknowledged or NACKed by the other end.
type resendRecord struct {
	pk            *packet
	timestamp     time.Time
	length        int
	retransmitted bool
}

// newRecoveryQueue returns a new initialised recovery queue.
func newRecoveryQueue() *resendMap {
	return &resendMap{
		unacknowledged: make(map[uint24]resendRecord),
	}
}

// add puts a packet at the index passed and records the current time.
func (m *resendMap) add(index uint24, pk *packet, length int, retransmitted bool) {
	if old, ok := m.unacknowledged[index]; ok {
		m.inFlightBytes -= old.length
	}
	// A retransmitted record is RTT-poisoned for Karn's algorithm. Resends
	// always pass retransmitted=true, so replacing the record preserves that
	// state for the new sequence number.
	m.unacknowledged[index] = resendRecord{pk: pk, timestamp: time.Now(), length: length, retransmitted: retransmitted}
	m.inFlightBytes += length
}

// acknowledge marks a packet with the index passed as acknowledged. The packet
// is removed from the resendMap and returned if found.
func (m *resendMap) acknowledge(index uint24) (resendRecord, bool) {
	return m.remove(index, 1)
}

// retransmit looks up a packet with an index from the resendMap so that it may
// be resent.
func (m *resendMap) retransmit(index uint24) (*packet, bool) {
	record, ok := m.remove(index, 2)
	return record.pk, ok
}

// remove deletes an index from the resendMap and adds the time since the
// packet was originally sent multiplied by mul to the delays slice.
func (m *resendMap) remove(index uint24, mul int) (resendRecord, bool) {
	record, ok := m.unacknowledged[index]
	if !ok {
		return resendRecord{}, false
	}
	delete(m.unacknowledged, index)
	m.inFlightBytes -= record.length
	if m.inFlightBytes < 0 {
		m.inFlightBytes = 0
	}

	now := time.Now()
	if !record.retransmitted {
		m.delays = append(m.delays, delayRecord{timestamp: now, delay: now.Sub(record.timestamp) * time.Duration(mul)})
	}
	return record, true
}

// rtt returns the user-facing average round trip time between the putting of a
// value into the recovery queue and the taking out of it again. Congestion
// control intentionally uses a separate EWMA estimator in congestionWindow.
func (m *resendMap) rtt(now time.Time) time.Duration {
	const rttCalculationWindow = time.Second * 5
	n := 0
	for _, record := range m.delays {
		if now.Sub(record.timestamp) <= rttCalculationWindow {
			m.delays[n] = record
			n++
		}
	}
	clear(m.delays[n:])
	m.delays = m.delays[:n]
	if len(m.delays) == 0 {
		// No records yet, generally should not happen. Just return a reasonable
		// amount of time.
		return time.Millisecond * 50
	}
	var total time.Duration
	for _, record := range m.delays {
		total += record.delay
	}
	return total / time.Duration(len(m.delays))
}

func (m *resendMap) inFlight() int {
	return m.inFlightBytes
}
