package yule

import (
	"reflect"
	"sync"
)

type Runnable struct {
	pipeline *Pipeline
	metadata *Metadata
}

func newRunnable(metadata *Metadata) *Runnable {

	runnable := new(Runnable)
	runnable.metadata = metadata

	runnable.pipeline = new(Pipeline)
	runnable.pipeline.Identifier = metadata.Deployment.Identifier

	// todo : this mem allocation should not be here
	runnable.pipeline.Stats = NewStatistics(len(metadata.Deployment.Functions), len(metadata.Deployment.Pipes))

	runnable.pipeline.Channels = make([]*pChannel, len(metadata.Deployment.Pipes))
	for i, c := range metadata.Deployment.Pipes {

		newChan := new(pChannel)
		newChan.Identifier = c.Identifier
		newChan.Config.Threshold = c.Threshold
		newChan.Config.GrowthFactor = c.GrowthFactor
		newChan.Stats = &runnable.pipeline.Stats.Pipes[i]
		newChan.Value = New(newChan.Identifier, c.Threshold, c.GrowthFactor, &runnable.pipeline.Stats.Pipes[i].Timing)
		newChan.Receiver = make([]*pFunction, 0)
		newChan.Producers = make([]*pFunction, 0)

		runnable.pipeline.Channels[i] = newChan
	}

	runnable.pipeline.Roots = make([]*pFunction, 0)
	runnable.pipeline.Tails = make([]*pFunction, 0)
	runnable.pipeline.Functions = make([]*pFunction, len(metadata.Deployment.Functions))
	for i, f := range metadata.Deployment.Functions {

		function := new(pFunction)
		function.Identifier = f.Identifier
		if f.StartWith != 0 {
			function.Config.StartWith = f.StartWith
		} else {
			function.Config.StartWith = 1
		}
		function.Config.WaitBefore = f.WaitBefore
		function.Stats = &runnable.pipeline.Stats.Functions[i]
		function.Quit = make([]chan bool, 0)
		function.Value = metadata.Functions[i].value
		function.Reflected.Value = reflect.ValueOf(function.Value)
		function.Reflected.Type = reflect.TypeOf(function.Value)

		function.To = nil
		function.From = nil
		for _, c := range runnable.pipeline.Channels {
			if c.Identifier == f.To {
				c.Producers = append(c.Producers, function)
				function.To = c
			}

			if c.Identifier == f.From {
				c.Receiver = append(c.Receiver, function)
				function.From = c
			}
		}

		if function.To == nil {
			runnable.pipeline.Tails = append(runnable.pipeline.Tails, function)
		}

		if function.From == nil {
			runnable.pipeline.Roots = append(runnable.pipeline.Tails, function)
		}

		if function.Identifier == metadata.Deployment.OnStartup {
			runnable.pipeline.OnStartup = function
		}

		if function.Identifier == metadata.Deployment.OnTeardown {
			runnable.pipeline.OnTeardown = function
		}

		runnable.pipeline.Functions[i] = function
	}

	return runnable
}

func (runnable *Runnable) RunWithMetadata(metadata ...map[string]string) error {

	var m map[string]string
	if len(metadata) == 0 {
		m = make(map[string]string)
	} else {
		m = metadata[0]
	}

	instance := newInstance(runnable.pipeline, m)
	return instance.Start(false)
}

func (runnable *Runnable) Run(injectables ...any) error {

	m := make(map[string]string)
	instance := newInstance(runnable.pipeline, m, injectables...)
	return instance.Start(false)
}

func (runnable *Runnable) Snapshot() *Statistics {

	return runnable.pipeline.Stats
}

type TestReport struct {
	Success     bool   `json:"success"`
	FailedStep  int    `json:"step"`
	FailedCause string `json:"cause"`
}

func (runnable *Runnable) Test(data any, injectables ...any) TestReport {

	m := make(map[string]string)
	instance := newInstance(runnable.pipeline, m, injectables...)

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		instance.Start(true)
		wg.Done()
	}()

	instance.send(data)
	instance.close()

	wg.Wait()

	return TestReport{}
}
