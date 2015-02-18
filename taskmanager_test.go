package sequence

import (
	"sync"
	"testing"
	"time"
)

func TestNewTaskManager(t *testing.T) {
	tm := NewTaskManager(defaultIndexSize)
	if tm == nil {
		t.Error("NewTaskManager() failed.")
	}
}

func TestNewTaskManagerStruct(t *testing.T) {
	tm := newTaskManager(defaultIndexSize)
	if tm.seqToIndexFunc == nil {
		t.Error("seqToIndexFunc should be initialized.")
	}
}

func TestAddHandler(t *testing.T) {
	tm := newTaskManager(defaultIndexSize)
	f := func(id SequenceID, index int) {}
	tm.AddHandler(f)
	if len(tm.handlerGroups) != 1 {
		t.Error("AddHandler failed to add a new task. len(ct.handlerGroups):", len(tm.handlerGroups))
	}
}

func TestAddHandlers(t *testing.T) {
	tm := newTaskManager(defaultIndexSize)
	f := func(id SequenceID, index int) {}
	for i := 0; i < 10; i++ {
		tm.AddHandler(f)
	}
	if len(tm.handlerGroups) != 10 {
		t.Error("AddHandler cannot add several tasks.", len(tm.handlerGroups))
	}
}

func TestPutTask(t *testing.T) {
	tm := newTaskManager(2)
	value := -1
	currentID := -1
	f := func(id SequenceID, index int) {
		currentID = int(id)
		value = index
	}
	tm.PutTask(f)
	if value != 0 || currentID != 0 {
		t.Error("PutTask should call initialier and set index.")
	}
	tm.PutTask(f)
	if value != 1 || currentID != 1 {
		t.Error("PutTask should call initializer and set a new index.")
	}
	tm.PutTask(f)
	if value != 0 || currentID != 2 {
		t.Error("handler's index should be less than size.")
	}
}

func TestBasicBehavior(t *testing.T) {
	count := 0
	f := func(id SequenceID, index int) {
		count++
	}

	tm := NewTaskManager(defaultIndexSize)
	tm.AddHandler(f)
	tm.Start()
	tm.PutTask(nil)
	tm.Stop()
	if count != 1 {
		t.Error("TaskManager behavior has a problem. count:", count)
	}
}

func TestSomeHandlersBehavior(t *testing.T) {
	var m sync.Mutex
	count := 0
	f := func(id SequenceID, index int) {
		m.Lock()
		defer m.Unlock()
		count++
	}

	tm := NewTaskManager(defaultIndexSize)
	tm.AddHandler(f)
	tm.AddHandler(f)
	tm.AddHandler(f)
	tm.AddHandler(f)
	tm.AddHandler(f)
	tm.Start()
	tm.PutTask(nil)
	tm.PutTask(nil)
	tm.Stop()

	if count != 10 {
		t.Error("TaskManager behavior has a problem. count:", count)
	}
}

func TestWaitingPutTask(t *testing.T) {
	values := make([]int, 4, 4)
	handler := func(id SequenceID, index int) {
		time.Sleep(10)
		values[index] += 1
	}

	tm := NewTaskManager(4)
	tm.AddHandler(handler)
	tm.Start()
	for i := 0; i < 10; i++ {
		tm.PutTask(func(id SequenceID, index int) {
			values[index] = 0
		})
	}
	tm.Stop()

	for _, v := range values {
		if v != 1 {
			t.Error("Value should be accessed from 1 handler at the same time.", v)
		}
	}
}

func BenchmarkHandler(b *testing.B) {
	f := func(id SequenceID, index int) {
		index++
	}

	tm := NewTaskManager(defaultIndexSize)
	tm.AddHandler(f)
	tm.Start()
	for i := 0; i < 1000000; i++ {
		tm.PutTask(nil)
	}
	tm.Stop()
}

func BenchmarkPutTask(b *testing.B) {
	count := 500000
	values := make([]int, defaultIndexSize)
	handler := func(id SequenceID, index int) {
		values[index] += 1
	}
	tm := NewTaskManager(defaultIndexSize)
	tm.AddHandler(handler).Then(handler).Then(handler)
	tm.Start()
	for i := 0; i < count; i++ {
		tm.PutTask(func(id SequenceID, index int) {
			values[index] = 0
		})
	}
	tm.Stop()
	for _, value := range values {
		if value != 3 {
			b.Error("Error result is incorrect. value:", value)
		}
	}
}
