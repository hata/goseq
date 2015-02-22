package goseq

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestNewExecutor(t *testing.T) {
	ex := newExecutor(4)
	defer ex.Stop()
	if ex.max != 4 {
		t.Error("NewExecutor should initialize max value.")
	}
	if len(ex.jobs) != 4 {
		t.Error("NewExecutor should initialize jobs table.")
	}
	if len(ex.resultChannels) != 4 {
		t.Error("NewExecutor should initialize resultChannels table.")
	}
	if ex.jobIndexChannel == nil {
		t.Error("NewExecutor should initialize jobIndexChannel.")
	}
	if ex.freeJobIndexChannel == nil {
		t.Error("NewExecutor should initialize freeJobIndexChannel.")
	}
	if len(ex.freeJobIndexChannel) != 4 {
		t.Error("NewExecutor should have free list.")
	}
}

func TestMax(t *testing.T) {
	ex := newExecutor(4)
	defer ex.Stop()
	if ex.Max() != 4 {
		t.Error("Executor.Max should return a max value.")
	}
}

func TestExecute(t *testing.T) {
	ex := newExecutor(4)
	defer ex.Stop()
	f := ex.Execute(func() (Any, error) {
		time.Sleep(10)
		return "finished", nil
	})
	res, err := f.Result()
	resText, ok := res.(string)
	if !ok || resText != "finished" || err != nil {
		t.Error("Execute should provide a future instance.")
	}
	res, err = f.Result()
	resText, ok = res.(string)
	if !ok || resText != "finished" || err != nil {
		t.Error("Future should return the same contents again.")
	}
}

func createJobFunc(i int, errText string) Job {
	return func() (Any, error) {
		time.Sleep(10)
		return fmt.Sprintf("finished%d", i), errors.New(errText)
	}
}

func TestRunFuture(t *testing.T) {
	ex := newExecutor(2)
	defer ex.Stop()

	f1 := ex.Execute(createJobFunc(1, ""))
	f2 := ex.Execute(createJobFunc(2, ""))

	result1, _ := f1.Result()
	result2, _ := f2.Result()

	if result1.(string) != "finished1" || result2.(string) != "finished2" {
		t.Error("Get finished.")
	}
}

func TestAddSomeJobs(t *testing.T) {
	ex := newExecutor(2)
	defer ex.Stop()
	results := make([]Future, 5)
	for i := 0; i < 5; i++ {
		results[i] = ex.Execute(createJobFunc(i, fmt.Sprintf("error%d", i)))
	}
	for i := 0; i < 5; i++ {
		expected := fmt.Sprintf("finished%d", i)
		expectedError := fmt.Sprintf("error%d", i)
		res, err := results[i].Result()
		resText := res.(string)
		errText := err.Error()
		if resText != expected || errText != expectedError {
			t.Error("Finished value is incorrect.")
		}
	}
}

func TestNewFuture(t *testing.T) {
	f := newFuture()
	if f.finished {
		t.Error("future.finished should be false.")
	}
	if f.waitChannel == nil {
		t.Error("future.waitChannel should be initialized.")
	}
}
