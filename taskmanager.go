package sequence

import (
	"time"
)

const (
	initialTasksCap  = 4
	defaultIndexSize = 1024
)

type TaskHandler func(id SequenceID, index int)
type SequenceIDToIndexFunc func(id SequenceID) (index int)

type TaskManager interface {
	PutTask(initHandler TaskHandler) SequenceID
	AddHandler(handler TaskHandler) HandlerGroup
	AddHandlers(handler TaskHandler, handlers ...TaskHandler) HandlerGroup
	Start()
	Stop()
}

type taskManager struct {
	seqToIndexFunc      SequenceIDToIndexFunc
	handlerGroups       []HandlerGroup
	size                SequenceID
	indexMask           SequenceID
	currentID           SequenceID
	cachedMinSequenceID SequenceID
}

func SequenceIDToIndexFuncImpl(id SequenceID) (index int) {
	return int((defaultIndexSize - 1) & int64(id))
}

// Create a new TaskManager instance.
// size should be pow2 like 2,4,8,16, ...
func NewTaskManager(size int) TaskManager {
	return newTaskManager(size)
}

func newTaskManager(size int) (tm *taskManager) {
	tm = new(taskManager)
	tm.currentID = initialSequenceValue
	tm.cachedMinSequenceID = initialSequenceValue
	tm.size = SequenceID(size)
	tm.indexMask = SequenceID(size - 1)
	tm.handlerGroups = make([]HandlerGroup, 0, initialTasksCap)
	tm.seqToIndexFunc = func(id SequenceID) int {
		return int(tm.indexMask & id)
	}
	return
}

// Support single thread only.
func (tm *taskManager) PutTask(initHandler TaskHandler) SequenceID {
	current := tm.currentID
	nextID := current + 1
	wrapPoint := nextID - tm.size
	cachedMinSequenceID := tm.cachedMinSequenceID

	if wrapPoint > cachedMinSequenceID || cachedMinSequenceID > current {
		var minSequenceID SequenceID
		for {
			minSequenceID := tm.getMinimumLastProcessedID(current)
			if wrapPoint > minSequenceID {
				time.Sleep(1)
			} else {
				break
			}
		}
		tm.cachedMinSequenceID = minSequenceID
	}
	tm.currentID = nextID

	defer tm.put(nextID)

	if initHandler != nil {
		initHandler(nextID, tm.seqToIndexFunc(nextID))
	}
	return nextID
}

// This can support a single thread operation only because
// multi thread call may put different order.
func (tm *taskManager) put(id SequenceID) {
	for _, group := range tm.handlerGroups {
		group.process(id)
	}
}

func (tm *taskManager) AddHandler(handler TaskHandler) HandlerGroup {
	return tm.AddHandlers(handler)
}

func (tm *taskManager) AddHandlers(handler TaskHandler, handlers ...TaskHandler) HandlerGroup {
	group := newHandlerGroup()
	group.seqToIndexFunc = tm.seqToIndexFunc
	group.addHandlers(handler, handlers...)
	tm.handlerGroups = append(tm.handlerGroups, group)
	return group
}

func (tm *taskManager) Start() {
	for _, group := range tm.handlerGroups {
		group.startAll()
	}
}

func (tm *taskManager) Stop() {
	for _, group := range tm.handlerGroups {
		group.stopAll()
	}
}

func (tm *taskManager) getMinimumLastProcessedID(minimum SequenceID) SequenceID {
	processedID := minimum
	for _, handlerGroup := range tm.handlerGroups {
		for _, lastGroup := range handlerGroup.lastHandlerGroups() {
			n := lastGroup.LastProcessedID()
			if n < processedID {
				processedID = n
			}
		}
	}
	return processedID
}
