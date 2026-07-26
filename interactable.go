// Package ScalingFunctions
//
// Copyright (c) 2024-2026. Gabriel Cordovado
// All rights reserved.
//
// Source file:  interactable.go
package ScalingFunctions

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
	return interactable.runtime.Status
}

func (interactable Interactable) IsRunning() bool {
	return !((interactable.runtime.Status == Terminated) || (interactable.runtime.Status == Failed))
}

func (interactable Interactable) Stop() {

	interactable.runtime.close()
}

func (interactable Interactable) GetStatistics() *Statistics {

	return interactable.pipeline.Stats
}
