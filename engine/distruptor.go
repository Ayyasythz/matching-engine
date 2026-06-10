package engine

import (
	"runtime"
	"sync/atomic"
)

const cacheLine = 64

type paddedSeq struct {
	value int64
	_     [cacheLine - 8]byte
}

type RingBuffer struct {
	_     [cacheLine]byte
	cap   int64
	mask  int64
	shift uint
	_     [cacheLine]byte

	prod      paddedSeq
	cons      paddedSeq
	slots     []command
	available []int32
}

func NewRingBuffer(cap int64) *RingBuffer {
	if cap <= 0 || (cap&(cap-1)) != 0 {
		panic("distruptor: capacity must be a positive power of 2")
	}

	shift := uint(0)
	for int64(1)<<shift < cap {
		shift++
	}
	avail := make([]int32, cap)
	for i := range avail {
		avail[i] = -1
	}

	rb := &RingBuffer{
		cap:       cap,
		mask:      cap - 1,
		shift:     shift,
		slots:     make([]command, cap),
		available: avail,
	}
	atomic.StoreInt64(&rb.prod.value, -1)
	atomic.StoreInt64(&rb.cons.value, -1)
	return rb
}

func (rb *RingBuffer) Claim() int64 {
	seq := atomic.AddInt64(&rb.prod.value, 1)
	wrapPoint := seq - rb.cap
	for {
		gate := atomic.LoadInt64(&rb.cons.value)
		if gate >= wrapPoint {
			break
		}
		runtime.Gosched()
	}
	return seq
}

func (rb *RingBuffer) Write(seq int64, cmd command) {
	rb.slots[seq&rb.mask] = cmd
}

func (rb *RingBuffer) Publish(seq int64) {
	round := int32(seq >> rb.shift)
	atomic.StoreInt32(&rb.available[seq&rb.mask], round)
}

func (rb *RingBuffer) TryNext() (int64, command, bool) {
	nextSeq := atomic.LoadInt64(&rb.cons.value) + 1
	slot := nextSeq & rb.mask
	round := int32(nextSeq >> rb.shift)

	if atomic.LoadInt32(&rb.available[slot]) != round {
		return 0, command{}, false
	}

	return nextSeq, rb.slots[slot], true
}

func (rb *RingBuffer) Advance(seq int64) {
	atomic.StoreInt64(&rb.cons.value, seq)
}
