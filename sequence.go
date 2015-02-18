package sequence

import "sync/atomic"

const (
	initialSequenceValue = -1
)

type SequenceID int64

type Sequence interface {
	Get() SequenceID
	Set(newSequenceID SequenceID)
	Next() SequenceID
}

type sequence struct {
	value int64
}

func NewSequence() Sequence {
	return newSequence()
}

func newSequence() (seq *sequence) {
	seq = new(sequence)
	seq.set(initialSequenceValue)
	return
}

func (seq *sequence) Get() SequenceID {
	return SequenceID(seq.get())
}

func (seq *sequence) Set(newSequenceID SequenceID) {
	seq.set(int64(newSequenceID))
}

func (seq *sequence) Next() SequenceID {
	return SequenceID(seq.incrementAndGet())
}

func (seq *sequence) get() int64 {
	return atomic.LoadInt64(&seq.value)
}

func (seq *sequence) set(v int64) {
	atomic.StoreInt64(&seq.value, v)
}

func (seq *sequence) incrementAndGet() int64 {
	return atomic.AddInt64(&seq.value, 1)
}

func (seq *sequence) compareAndSet(expected int64, v int64) bool {
	return atomic.CompareAndSwapInt64(&seq.value, expected, v)
}

func (seq *sequence) addAndGet(delta int64) int64 {
	var current, newvalue int64
	for {
		current = seq.get()
		newvalue = current + delta
		if seq.compareAndSet(current, newvalue) {
			return newvalue
		}
	}
}
