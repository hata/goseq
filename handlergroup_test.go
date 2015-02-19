package sequence

import (
	"sync"
	"testing"
	"time"
)

func sampleToIndexFunc(id SequenceID) (index int) {
	index = 0xff & int(id)
	return
}

func TestNewHandlerGroupStruct(t *testing.T) {
	group := newHandlerGroup(sampleToIndexFunc)
	if group.handlers == nil {
		t.Error("handlers should be initialized.")
	}
	if group.nextGroups == nil {
		t.Error("nextGroups should be initialized.")
	}
	if group.lastProcessedID == nil {
		t.Error("lastProcessedID should be initialized.")
	}
	if group.seqToIndexFunc == nil {
		t.Error("seqToIndexFunc should not be nil.")
	}
}

func TestAddHandlerHandlerGroup(t *testing.T) {
	group := newHandlerGroup(sampleToIndexFunc)
	f := func(id SequenceID, index int) {}
	group.AddHandler(f)
	if len(group.handlers) != 1 {
		t.Error("AddHandler should add a new handler.")
	}
	group.AddHandler(f, f)
	if len(group.handlers) != 3 {
		t.Error("AddHandler can add some tasks.")
	}
}

func TestAddhandlersHandlerGroups(t *testing.T) {
	group := newHandlerGroup(sampleToIndexFunc)
	f := func(id SequenceID, index int) {}
	handlers := make([]TaskHandler, 4, 4)
	for i := range handlers {
		handlers[i] = f
	}
	group.AddHandlers(handlers)
	if len(group.handlers) != 4 {
		t.Error("AddHandlers should have added handlers.")
	}
}

func TestAddNextGroup(t *testing.T) {
	group := newHandlerGroup(sampleToIndexFunc)
	group2 := newHandlerGroup(sampleToIndexFunc)

	group.addNextGroup(group2)
	if len(group.nextGroups) != 1 {
		t.Error("addNextGroup should add a new group.")
	}
}

func TestAddNextGroups(t *testing.T) {
	group := newHandlerGroup(sampleToIndexFunc)
	group2 := newHandlerGroup(sampleToIndexFunc)
	group3 := newHandlerGroup(sampleToIndexFunc)
	group4 := newHandlerGroup(sampleToIndexFunc)

	group.addNextGroups(group2)
	group.addNextGroups(group3, group4)
	if len(group.nextGroups) != 3 {
		t.Error("addNextGroup should add a new group.")
	}
}

func TestStartAndStop(t *testing.T) {
	group := newHandlerGroup(sampleToIndexFunc)
	handler := func(id SequenceID, index int) {}
	group.AddHandler(handler)
	group.start()
	group.process(1)
	group.stop()
	if group.LastProcessedID() != 1 {
		t.Error("start/stop didn't process a request.")
	}
	if group.inChannels != nil {
		t.Error("stop should close and clean inChannels.")
	}
	if group.outChannels != nil {
		t.Error("stop should close and clean outChannels.")
	}
}

func TestStartAllAndStopAll(t *testing.T) {
	group := newHandlerGroup(sampleToIndexFunc)
	group2 := newHandlerGroup(sampleToIndexFunc)
	handler := func(id SequenceID, index int) {}
	group.AddHandler(handler)
	group2.AddHandler(handler)
	group.addNextGroup(group2)
	group.startAll()
	group.process(1)
	group.stopAll()

	if group.LastProcessedID() != 1 {
		t.Error("startAll/stopAll didn't process a request")
	}
	if group2.LastProcessedID() != 1 {
		t.Error("StartAll/StopAll didn't propagate to a next group.")
	}
	if group.inChannels != nil {
		t.Error("stop should close and clean inChannels.")
	}
	if group.outChannels != nil {
		t.Error("stop should close and clean outChannels.")
	}
	if group2.inChannels != nil {
		t.Error("stop should close and clean inChannels.")
	}
	if group2.outChannels != nil {
		t.Error("stop should close and clean outChannels.")
	}
}

func TestProcess(t *testing.T) {
	var m sync.Mutex
	count := 0
	group := newHandlerGroup(sampleToIndexFunc)
	handler := func(id SequenceID, index int) {
		m.Lock()
		defer m.Unlock()
		count++
	}
	group.AddHandler(handler)
	group.start()
	group.process(1)
	group.process(2)
	group.stop()

	if count != 2 {
		t.Error("process call doesn't handle requests.")
	}

	if group.LastProcessedID() != 2 {
		t.Error("LastProcessedID is not updated well.")
	}
}

func TestLastHandlerGroups(t *testing.T) {
	group := newHandlerGroup(sampleToIndexFunc)
	lastGroups := group.lastHandlerGroups()
	if len(lastGroups) != 1 || lastGroups[0] != group {
		t.Error("lastHandlerGroups() returns self when there is no nextGroups.")
	}
}

func TestLastHandlerGroupsReturnNextGroup(t *testing.T) {
	group := newHandlerGroup(sampleToIndexFunc)
	group2 := newHandlerGroup(sampleToIndexFunc)
	group.addNextGroup(group2)
	lastGroups := group.lastHandlerGroups()
	if len(lastGroups) != 1 || lastGroups[0] != group2 {
		t.Error("lastHandlerGroups() returns a next group. len:", len(lastGroups))
	}
}

func TestLastHandlerGroupsReturnSomeGroups(t *testing.T) {
	group := newHandlerGroup(sampleToIndexFunc)
	group2 := newHandlerGroup(sampleToIndexFunc)
	group3 := newHandlerGroup(sampleToIndexFunc)
	group4 := newHandlerGroup(sampleToIndexFunc)
	group5 := newHandlerGroup(sampleToIndexFunc)
	group.addNextGroups(group2, group3)
	group2.addNextGroups(group4, group5)
	lastGroups := group.lastHandlerGroups()
	if len(lastGroups) != 3 ||
		lastGroups[0] != group4 ||
		lastGroups[1] != group5 ||
		lastGroups[2] != group3 {
		t.Error("lastHandlerGroups() returns next groups. len:", len(lastGroups))
	}
}

func TestSomeHandlers(t *testing.T) {
	var m sync.Mutex
	count := 0
	group := newHandlerGroup(sampleToIndexFunc)
	handler := func(id SequenceID, index int) {
		m.Lock()
		defer m.Unlock()
		count++
	}
	group.AddHandler(handler, handler, handler, handler, handler)
	group.start()
	group.process(0)
	group.process(1)
	group.process(2)
	group.stop()
	if count != 15 { // 5 handlers and processing 3 times
		t.Error("handlers are not processed.")
	}
}

func TestSomeHandlersAndSomeNextGroups(t *testing.T) {
	var m sync.Mutex
	count := 0

	handler := func(id SequenceID, index int) {
		m.Lock()
		defer m.Unlock()
		count++
	}

	group1 := newHandlerGroup(sampleToIndexFunc)
	group1.name = "group1"
	group1.AddHandler(handler)
	group2 := newHandlerGroup(sampleToIndexFunc)
	group2.name = "group2"
	group2.AddHandler(handler, handler)
	group3 := newHandlerGroup(sampleToIndexFunc)
	group3.name = "group3"
	group3.AddHandler(handler, handler, handler)
	group4 := newHandlerGroup(sampleToIndexFunc)
	group4.name = "group4"
	group4.AddHandler(handler, handler, handler, handler)
	group5 := newHandlerGroup(sampleToIndexFunc)
	group5.name = "group5"
	group5.AddHandler(handler, handler, handler, handler, handler)

	group1.addNextGroup(group2)
	group1.addNextGroup(group3)
	group2.addNextGroup(group4)
	group4.addNextGroup(group5)

	group1.startAll()
	group1.process(0)
	group1.process(1)
	group1.process(2)
	group1.stopAll()

	time.Sleep(10000)

	if group1.LastProcessedID() != 2 {
		t.Error("LastProcessedID didn't return 2. id:", group3.LastProcessedID())
	}

	if group2.LastProcessedID() != 2 {
		t.Error("LastProcessedID didn't return 2. id:", group4.LastProcessedID())
	}

	if group3.LastProcessedID() != 2 {
		t.Error("LastProcessedID didn't return 2. id:", group3.LastProcessedID())
	}

	if group4.LastProcessedID() != 2 {
		t.Error("LastProcessedID didn't return 2. id:", group4.LastProcessedID())
	}

	if group5.LastProcessedID() != 2 {
		t.Error("LastProcessedID didn't return 2. id:", group5.LastProcessedID())
	}

	if count != (15 * 3) { // 15 handlers and 3 requests
		t.Error("Some handlers and some groups are not processing requests. count:", count)
	}
}

func BenchmarkProcess(b *testing.B) {
	group := newHandlerGroup(sampleToIndexFunc)
	seq := NewSequence()
	handler := func(id SequenceID, index int) {
		index++
	}
	group.AddHandler(handler)
	group.start()
	for i := 0; i < 1000000; i++ {
		group.process(seq.Next())
	}
	group.stop()
}

func BenchmarkSomeGroups(b *testing.B) {
	groups := make([]HandlerGroup, 10, 10)
	seq := NewSequence()
	handler := func(id SequenceID, index int) {
		index++
	}
	var previousGroup HandlerGroup
	for i, _ := range groups {
		groups[i] = newHandlerGroup(sampleToIndexFunc)
		groups[i].AddHandler(handler, handler)
		if previousGroup != nil {
			previousGroup.addNextGroup(groups[i])
		}
		previousGroup = groups[i]
	}

	groups[0].startAll()
	for i := 0; i < 100000; i++ {
		groups[0].process(seq.Next())
	}
	groups[0].stopAll()
}
