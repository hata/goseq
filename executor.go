package goseq

import "sync"

const (
	closeExecutorIndex = -1
)

type Any interface{}

type Job func() (Any, error)

type Executor interface {
	Max() int
	Execute(runnable Job) Future
	Stop()
}

type executor struct {
	lock                sync.Mutex
	max                 int
	freeJobIndexChannel chan int
	jobIndexChannel     chan int
	resultChannels      []chan Future
	jobs                []Job
	waitingStop         sync.WaitGroup
}

type Future interface {
	Result() (Any, error)
}

type future struct {
	finished    bool
	waitChannel chan bool
	result      Any
	err         error
}

func NewExecutor(max int) Executor {
	return newExecutor(max)
}

func newExecutor(max int) (ex *executor) {
	ex = new(executor)
	ex.max = max
	ex.jobs = make([]Job, max)
	ex.startWorkers()
	return ex
}

func (ex *executor) startWorkers() {
	ex.jobIndexChannel = make(chan int)
	ex.resultChannels = make([]chan Future, ex.max)
	ex.freeJobIndexChannel = make(chan int, ex.max)
	ex.waitingStop.Add(ex.max)
	for i := 0; i < ex.max; i++ {
		ex.resultChannels[i] = make(chan Future)
		go ex.startWorker(ex.jobIndexChannel, ex.resultChannels[i])
		ex.freeJobIndexChannel <- i
	}
}

func (ex *executor) Stop() {
	for i := 0;i < ex.max;i++ {
		ex.jobIndexChannel <- closeExecutorIndex
	}
	close(ex.jobIndexChannel)
	close(ex.freeJobIndexChannel)
	for i := range ex.jobs {
		ex.jobs[i] = nil
	}
	ex.waitingStop.Wait()
}

func newFuture() (f *future) {
	f = new(future)
	f.finished = false
	f.waitChannel = make(chan bool, 1)
	return f
}

func (ex *executor) startWorker(newJobIndexChannel <-chan int, resultChannel chan Future) {
	defer ex.waitingStop.Done()
	for {
		jobIndex := <-newJobIndexChannel
		if jobIndex < 0 {
			close(resultChannel)
			break
		}
		f := newFuture()
		resultChannel <- f
		f.result, f.err = ex.jobs[jobIndex]()
		f.waitChannel <- true
		close(f.waitChannel)
		ex.addFreeJobIndexChannel(jobIndex)
	}
}

// I'm not sure writing to channel needs to lock or not.
func (ex *executor) addFreeJobIndexChannel(jobIndex int) {
	ex.lock.Lock()
	defer ex.lock.Unlock()
	ex.freeJobIndexChannel <- jobIndex
}

func (ex *executor) Max() int {
	return ex.max
}

func (ex *executor) Execute(job Job) Future {
	index := <-ex.freeJobIndexChannel
	ex.jobs[index] = job
	ex.jobIndexChannel <- index
	return <-ex.resultChannels[index]
}

func (f *future) Result() (Any, error) {
	if !f.finished {
		f.finished = <-f.waitChannel
	}
	return f.result, f.err
}
