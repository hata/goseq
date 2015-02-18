package sequence

import "testing"

func TestNewSequence(t *testing.T) {
	seq := NewSequence()
	if seq.Next() != 0 {
		t.Error("New Sequence should generate 0 for the first id.")
	}
}

func TestNewSequenceStruct(t *testing.T) {
	seq := newSequence()
	if seq.value != initialSequenceValue {
		t.Error("Configured value should be the initial value")
	}
}

func TestGet(t *testing.T) {
	seq := NewSequence()
	id := seq.Get()
	if id != initialSequenceValue {
		t.Error("Get() returns a wrong value.")
	}
}

func TestNext(t *testing.T) {
	seq := NewSequence()
	id := seq.Next()
	if id != 0 {
		t.Error("First ID should be 0.")
	}
	id = seq.Next()
	if id != 1 {
		t.Error("Next() should increase 1")
	}

}

func TestGetStruct(t *testing.T) {
	seq := newSequence()
	id := seq.get()
	if id != initialSequenceValue {
		t.Error("get() should return an initialSequenceValue.")
	}
}

func TestSet(t *testing.T) {
	seq := newSequence()
	seq.set(1)
	id := seq.get()
	if id != 1 {
		t.Error("set() should set a new value.")
	}
}

func TestIncrementAndGet(t *testing.T) {
	seq := newSequence()
	id := seq.incrementAndGet()
	if id != 0 {
		t.Error("incrementAndGet() should increment 1 and return the value.")
	}
	id = seq.incrementAndGet()
	if id != 1 {
		t.Error("incrementAndGet() should increment 1 and return the value.")
	}
}

func TestCompareAndSetAndSuccess(t *testing.T) {
	seq := newSequence()
	id := seq.incrementAndGet()
	seq.compareAndSet(id, 2)
	id2 := seq.get()
	if id2 != 2 {
		t.Error("compareAndSet should return ", 2)
	}
}

func TestCompareAndSetAndIgnore(t *testing.T) {
	seq := newSequence()
	id := seq.incrementAndGet()
	seq.compareAndSet(id+1, 2)
	id2 := seq.get()
	if id != id2 {
		t.Error("compareAndSet should fail when expected is not match.")
	}
}

func TestAddAndGet(t *testing.T) {
	seq := newSequence()
	seq.incrementAndGet()
	id := seq.incrementAndGet()
	id2 := seq.addAndGet(3)
	if id2 != (id + 3) {
		t.Error("addAndSet() should add delta and return it.")
	}
	if id2 != seq.get() {
		t.Error("addAndSet() should set a new value.")
	}
}
