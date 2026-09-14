// Package ScalingFunctions
//
// Copyright (c) 2024-2026. Gabriel Cordovado
// All rights reserved.
//
// Source file:  interactable.go
package ScalingFunctions

import "sync"

////////////////////////////////////////////////////////////////////////
//							Interactable
////////////////////////////////////////////////////////////////////////
//
//	Interactable
//		∟ Id        : a unique id that may be self-assigned after receiving an injectable
//		∟ Pipeline  : an identifier for the graph of functions and channels
//		∟ pipeline  : used to track a graph of functions and channels
//		∟ runtime   : used to track a running pipeline
//
////////////////////////////////////////////////////////////////////////

// Interactable
// is an abstraction used to interact with a running pipeline.
type Interactable struct {
	Id       uint64
	Pipeline string
	pipeline Pipeline
	runtime  *pipelineRuntime
}

func (interactable Interactable) Run(injectables ...any) error {

	interactable.runtime.injectDependencies(injectables...)
	return interactable.runtime.start()
}

func (interactable Interactable) GetStatus() RunStatus {
	return interactable.runtime.getStatus()
}

func (interactable Interactable) IsRunning() bool {
	status := interactable.runtime.getStatus()
	return !((status == Terminated) || (status == Failed))
}

func (interactable Interactable) Stop() {

	interactable.runtime.close()
}

func (interactable Interactable) GetStatistics() *Statistics {

	return interactable.pipeline.getStatisticsSnapshot()
}

type TestReport struct {
	Success bool   `json:"success"`
	Step    string `json:"step"`
	Cause   error  `json:"cause"`
}

func (interactable Interactable) Test(data any, injectables ...any) TestReport {

	if interactable.pipeline.flags.interactableCreated {
		return TestReport{
			Success: false,
			Cause:   NonOwningPipeline,
		}
	}

	interactable.runtime.injectDependencies(injectables...)

	// instructs pipeline to enable testing
	interactable.runtime.isForTesting()

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		interactable.runtime.start()
		wg.Done()
	}()

	interactable.runtime.send(data)
	interactable.runtime.close()

	wg.Wait()

	return interactable.runtime.testingReport
}

func (interactable Interactable) TestAs(f string, data any, injectables ...any) TestReport {

	if interactable.pipeline.flags.interactableCreated {
		return TestReport{
			Success: false,
			Cause:   NonOwningPipeline,
		}
	}

	interactable.runtime.injectDependencies(injectables...)

	// instructs pipeline to enable testing
	interactable.runtime.isForTesting()

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		interactable.runtime.start()
		wg.Done()
	}()

	interactable.runtime.send(data, f)
	interactable.runtime.close()

	wg.Wait()

	return interactable.runtime.testingReport
}
