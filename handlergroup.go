/*
   Package sequence is to run small tasks sequencially using Goroutine.

   The current version of Go language cannot use generic type without type
   assertion. So, this library doesn't manage efficient usage for context
   instances. Instead of using a context instance, this library provides
   'index' value to use a cache index. The provided index is
   generated based on configured 'size' parameter for TaskManager and
   it is reused after finishing a previous task which uses 'index'.
*/
package sequence

import (
	"sync"
)

const (
	channelBufferSize           = 256
	stopCurrentHandlerGroupOnly = -1
	stopAllHandlerGroups        = -2
)

// HandlerGroup is to manage several TaskHandler instances.
// Added TaskHandlers are run at the same time when a new
// SequenceID is put. After finishing all tasks for the SequenceID,
// then next HandlerGroups are received the finished SequenceID.
type HandlerGroup interface {
	AddHandler(handler TaskHandler, handlers ...TaskHandler)
	AddHandlers(handlers []TaskHandler)
	Then(handler TaskHandler, handlers ...TaskHandler) HandlerGroup
	LastProcessedID() SequenceID

	start()
	stop()
	startAll()
	stopAll()
	waitStop()
	waitStopAll()

	process(id SequenceID)
	addNextGroup(nextGroup HandlerGroup)
	addNextGroups(nextGroup HandlerGroup, nextGroups ...HandlerGroup)

	lastHandlerGroups() []HandlerGroup

	numOfHandlers() int
}

type handlerGroup struct {
	name            string
	nextGroups      []HandlerGroup
	handlers        []TaskHandler
	inChannels      []chan SequenceID
	outChannels     []chan SequenceID
	lastProcessedID Sequence
	seqToIndexFunc  sequenceIDToIndexFunc
	waitingStart    sync.WaitGroup
	waitingStop     sync.WaitGroup
}

func newHandlerGroup(toIndexFunc sequenceIDToIndexFunc) (group *handlerGroup) {
	group = new(handlerGroup)
	group.handlers = make([]TaskHandler, 0, 2)
	group.nextGroups = make([]HandlerGroup, 0, 2)
	group.lastProcessedID = NewSequence()
	group.seqToIndexFunc = toIndexFunc
	return
}

func (group *handlerGroup) process(id SequenceID) {
	for _, ch := range group.inChannels {
		ch <- id
	}
}

// Add a TaskHandler or some TaskHandlers.
func (group *handlerGroup) AddHandler(handler TaskHandler, handlers ...TaskHandler) {
	group.handlers = append(group.handlers, handler)
	if len(handlers) > 0 {
		group.handlers = append(group.handlers, handlers...)
	}
}

// Add TaskHandlers from an slice of TaskHandler.
func (group *handlerGroup) AddHandlers(handlers []TaskHandler) {
	if handlers != nil {
		group.handlers = append(group.handlers, handlers...)
	}
}

func (group *handlerGroup) addNextGroup(nextGroup HandlerGroup) {
	group.nextGroups = append(group.nextGroups, nextGroup)
}

func (group *handlerGroup) addNextGroups(nextGroup HandlerGroup, nextGroups ...HandlerGroup) {
	group.nextGroups = append(group.nextGroups, nextGroup)
	if nextGroups != nil {
		group.nextGroups = append(group.nextGroups, nextGroups...)
	}
}

func (group *handlerGroup) processHandler(handler TaskHandler, inChannel <-chan SequenceID, outChannel chan<- SequenceID) {
	group.waitingStart.Done()
	defer group.waitingStop.Done()
	for {
		id := <-inChannel
		if id == stopCurrentHandlerGroupOnly || id == stopAllHandlerGroups {
			outChannel <- id
			break
		}
		handler(id, group.seqToIndexFunc(id))
		outChannel <- id
	}
}

func (group *handlerGroup) sendToNextGroups() {
	group.waitingStart.Done()
	defer group.waitingStop.Done()
	var id SequenceID
	if len(group.outChannels) > 0 {
		for {
			// This loop expects all ids are the same.
			for _, ch := range group.outChannels {
				id = <-ch
			}
			if id == stopCurrentHandlerGroupOnly {
				break
			}
			for _, nextGroup := range group.nextGroups {
				nextGroup.process(id)
			}
			if id == stopAllHandlerGroups {
				break
			}
			group.lastProcessedID.Set(id)
		}
	}
}

func (group *handlerGroup) start() {
	length := len(group.handlers)
	group.waitingStart.Add(length + 1)
	group.waitingStop.Add(length + 1)
	group.inChannels = make([]chan SequenceID, length)
	group.outChannels = make([]chan SequenceID, length)

	for i, handler := range group.handlers {
		group.inChannels[i] = make(chan SequenceID, channelBufferSize)
		group.outChannels[i] = make(chan SequenceID, channelBufferSize)
		go group.processHandler(handler, group.inChannels[i], group.outChannels[i])
	}

	go group.sendToNextGroups()
	group.waitingStart.Wait()
}

func (group *handlerGroup) startAll() {
	for _, nextGroup := range group.nextGroups {
		nextGroup.startAll()
	}
	group.start()
}

func (group *handlerGroup) stop() {
	group.process(stopCurrentHandlerGroupOnly)
	group.waitStop()
}

func (group *handlerGroup) stopAll() {
	group.process(stopAllHandlerGroups)
	group.waitStopAll()
}

func (group *handlerGroup) waitStop() {
	group.waitingStop.Wait()
	defer func() {
		group.inChannels = nil
		group.outChannels = nil
	}()

	for _,ch := range group.inChannels {
		close(ch)
	}
	for _, ch := range group.outChannels {
		close(ch)
	}
}

func (group *handlerGroup) waitStopAll() {
	group.waitStop()
	for _, nextGroup := range group.nextGroups {
		nextGroup.waitStopAll()
	}
}

func (group *handlerGroup) lastHandlerGroups() []HandlerGroup {
	groups := make([]HandlerGroup, 0, len(group.nextGroups))
	if len(group.nextGroups) == 0 {
		groups = append(groups, group)
	} else {
		for _, next := range group.nextGroups {
			groups = append(groups, next.lastHandlerGroups()...)
		}
	}
	return groups
}

func (group *handlerGroup) numOfHandlers() int {
	return len(group.handlers)
}

// Create a new HandlerGroup and then add new TaskHandler instances to the
// new HandlerGroup. And then return the new handler. This new added handlers
// are run after running current(group instance) HandlerGroup's TaskHandlers.
func (group *handlerGroup) Then(handler TaskHandler, handlers ...TaskHandler) HandlerGroup {
	newGroup := newHandlerGroup(group.seqToIndexFunc)
	newGroup.AddHandler(handler, handlers...)
	group.addNextGroups(newGroup)
	return newGroup
}

// Get finished SequenceID. The returned value means that all tasks are finished
// for the specific returned value or more smaller SequenceIDs.
func (group *handlerGroup) LastProcessedID() SequenceID {
	return group.lastProcessedID.Get()
}
