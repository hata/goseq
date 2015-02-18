package sequence

import (
	"sync"
)

const (
	channelBufferSize           = 128
	stopCurrentHandlerGroupOnly = -1
	stopAllHandlerGroups        = -2
)

type HandlerGroup interface {
	Then(handler TaskHandler, handlers ...TaskHandler) HandlerGroup
	LastProcessedID() SequenceID

	start()
	stop()
	startAll()
	stopAll()
	waitStop()
	waitStopAll()

	process(id SequenceID)
	addHandler(handler TaskHandler)
	addHandlers(handler TaskHandler, handlers ...TaskHandler)
	addNextGroup(nextGroup HandlerGroup)
	addNextGroups(nextGroup HandlerGroup, nextGroups ...HandlerGroup)

	lastHandlerGroups() []HandlerGroup
}

type handlerGroup struct {
	name            string
	nextGroups      []HandlerGroup
	handlers        []TaskHandler
	inChannels      []chan SequenceID
	outChannels     []chan SequenceID
	lastProcessedID Sequence
	seqToIndexFunc  SequenceIDToIndexFunc
	waitingStart    sync.WaitGroup
	waitingStop     sync.WaitGroup
}

func NewHandlerGroup() HandlerGroup {
	return newHandlerGroup()
}

func newHandlerGroup() (group *handlerGroup) {
	group = new(handlerGroup)
	group.handlers = make([]TaskHandler, 0, 2)
	group.nextGroups = make([]HandlerGroup, 0, 2)
	group.lastProcessedID = NewSequence()
	group.seqToIndexFunc = SequenceIDToIndexFuncImpl
	return
}

func (group *handlerGroup) process(id SequenceID) {
	for _, ch := range group.inChannels {
		ch <- id
	}
}

func (group *handlerGroup) addHandler(handler TaskHandler) {
	group.handlers = append(group.handlers, handler)
}

func (group *handlerGroup) addHandlers(handler TaskHandler, handlers ...TaskHandler) {
	group.handlers = append(group.handlers, handler)
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

func (group *handlerGroup) Then(handler TaskHandler, handlers ...TaskHandler) HandlerGroup {
	newGroup := newHandlerGroup()
	newGroup.addHandlers(handler, handlers...)
	group.addNextGroups(newGroup)
	return newGroup
}

func (group *handlerGroup) LastProcessedID() SequenceID {
	return group.lastProcessedID.Get()
}
