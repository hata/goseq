package sequence

import (
	"time"
)

const (
	initialTasksCap = 4
)

// This type is defined acceptable function. 1st argument is to set
// a current SequenceID and 2nd 'index' value is to generate from
// 'id' value. index is between 0 and (size -1).
type TaskHandler func(id SequenceID, index int)

// Manage several TaskHandlers.
// Create a new instance using NewTaskManager() and then
// add TaskHandlers. And then, call Start() method to setup
// channels for each handlers. After that, call PutTask
// to initiate a new SequenceID and run tasks.
// Current version can support a single thread to call Put method.
type TaskManager interface {
	Put(initHandler TaskHandler) SequenceID
	AddHandler(handler TaskHandler, handlers ...TaskHandler) HandlerGroup
	AddHandlers(handlers []TaskHandler) HandlerGroup
	Start()
	Stop()
}

type sequenceIDToIndexFunc func(id SequenceID) (index int)

type taskManager struct {
	seqToIndexFunc      sequenceIDToIndexFunc
	handlerGroups       []HandlerGroup
	size                SequenceID
	indexMask           SequenceID
	currentID           SequenceID
	cachedMinSequenceID SequenceID
}

// Create a new TaskManager instance.
// size is required to set 2^x like 2,4,8,16, ...
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

// This method gets a new SequenceID and then call initHandler to initialize
// for the new SequenceID/index parameters. And then start calling TaskHandlers
// in different Goroutine. Current method can support only for a single thread
// usage to call this method. If 'index' is still used, then this method
// block the call until 'index' becomes available state.
func (tm *taskManager) Put(initHandler TaskHandler) SequenceID {
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

func (tm *taskManager) AddHandler(handler TaskHandler, handlers ...TaskHandler) HandlerGroup {
	group := newHandlerGroup(tm.seqToIndexFunc)
	if handler != nil {
		group.AddHandler(handler)
	}
	if len(handlers) > 0 {
		group.AddHandlers(handlers)
	}
	tm.handlerGroups = append(tm.handlerGroups, group)
	return group
}

func (tm *taskManager) AddHandlers(handlers []TaskHandler) HandlerGroup {
	return tm.AddHandler(nil, handlers...)
}

// Start all configured channels. Don't add new handlers/groups after starting handlers.
func (tm *taskManager) Start() {
	for _, group := range tm.handlerGroups {
		group.startAll()
	}
}

// Stop all configured channels. This is blocked until finishing all goroutines.
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
