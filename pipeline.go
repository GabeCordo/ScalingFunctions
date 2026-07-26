// Package ScalingFunctions
//
// Copyright (c) 2024-2026. Gabriel Cordovado
// All rights reserved.
//
// Source file:  pipeline.go
package ScalingFunctions

import (
	"errors"
	"reflect"
	"sync"
)

////////////////////////////////////////////////////////////////////////
//							Pipeline Types
////////////////////////////////////////////////////////////////////////
//
//	Pipeline
//		∟ pChannel
//			∟ pChannelConfig  : used to build a pChannel
//		∟ pFunction
//			∟ pFunctionConfig : used to build a pFunction
//
////////////////////////////////////////////////////////////////////////

var NonOwningPipeline = errors.New("function calls on a pipeline are not allowed after an interactable is created")

const pDefaultId uint64 = 0

type pInjectible struct {
	value reflect.Value
}

// pChannelConfig
// holds data required to build a pChannel.
type pChannelConfig struct {
	p *PipeIR
	s *PipeStatistic
}

// pChannel
// represents a connection between one-to-many functions.
type pChannel struct {
	Identifier string
	Producers  []*pFunction   `json:"-"`
	Receiver   []*pFunction   `json:"-"`
	Stats      *PipeStatistic `json:"-"`
	Value      *managedChannel

	Config struct {
		GrowthFactor float64
		Threshold    uint32
	} `json:"-"`

	Mutex sync.RWMutex `json:"-"`
}

// pFunctionConfig
// holds data required to build a pFunction.
type pFunctionConfig struct {
	f        *FunctionIR
	s        *FunctionStatistic
	channels []*pChannel
}

// pFunction
// represents a callable function that performs a unit of work on data.
type pFunction struct {
	Identifier string
	To         *pChannel
	From       *pChannel
	Stats      *FunctionStatistic `json:"-"`
	Quit       []chan bool        `json:"-"`

	Value     any `json:"-"`
	Reflected struct {
		Value reflect.Value
		Type  reflect.Type
	} `json:"-"`

	Config struct {
		StartWith  uint16
		WaitBefore bool
		Maximum    uint16
	} `json:"-"`

	Mutex sync.RWMutex `json:"-"`
}

// Pipeline
// represents a graph of pFunction and pChannel linked together.
type Pipeline struct {
	identifier string

	roots []*pFunction
	tails []*pFunction

	channels  []*pChannel
	functions []*pFunction

	onStartup  *pFunction
	onTeardown *pFunction

	injected      []*pInjectible
	numOfInjected int

	flags struct {
		interactableCreated bool
	}

	Stats *Statistics
}

////////////////////////////////////////////////////////////////////////
//						Pipeline Build functions
////////////////////////////////////////////////////////////////////////
//
//	buildPipeline
//		∟ buildPChannel
//		∟ buildPFunction
//
////////////////////////////////////////////////////////////////////////

func buildPChannel(config pChannelConfig) (*pChannel, error) {

	newChan := new(pChannel)
	var err error

	newChan.Identifier = config.p.Identifier
	newChan.Config.Threshold = config.p.Threshold
	newChan.Config.GrowthFactor = config.p.GrowthFactor
	newChan.Stats = config.s
	newChan.Value, err = newManagedChannel(newChan.Identifier, config.p.Threshold, config.p.GrowthFactor, &config.s.Timing)
	if err != nil {
		return nil, err
	}
	newChan.Receiver = make([]*pFunction, 0)
	newChan.Producers = make([]*pFunction, 0)

	return newChan, nil
}

func buildPFunction(config pFunctionConfig) (*pFunction, error) {

	function := new(pFunction)

	function.Identifier = config.f.Identifier
	if config.f.StartWith != 0 {
		function.Config.StartWith = config.f.StartWith
	} else {
		function.Config.StartWith = 1
	}
	function.Config.WaitBefore = config.f.WaitBefore
	function.Config.Maximum = config.f.Maximum
	function.Stats = config.s
	function.Quit = make([]chan bool, 0)
	function.Value = config.f.value
	function.Reflected.Value = reflect.ValueOf(function.Value)
	function.Reflected.Type = reflect.TypeOf(function.Value)

	function.To = nil
	function.From = nil
	for _, c := range config.channels {
		if c.Identifier == config.f.To {
			c.Producers = append(c.Producers, function)
			function.To = c
		}

		if c.Identifier == config.f.From {
			c.Receiver = append(c.Receiver, function)
			function.From = c
		}
	}

	return function, nil
}

func buildPipeline(iR *PipelineIR) (Pipeline, error) {

	graph := Pipeline{}
	graph.identifier = iR.Identifier

	// todo : this mem allocation should not be here
	graph.Stats = NewStatistics(len(iR.Functions), len(iR.Pipes))

	graph.channels = make([]*pChannel, len(iR.Pipes))
	for i, c := range iR.Pipes {
		config := pChannelConfig{
			p: &c,
			s: &graph.Stats.Pipes[i],
		}
		newChan, err := buildPChannel(config)
		if err != nil {
			return Pipeline{}, err
		}
		graph.channels[i] = newChan
	}

	graph.roots = make([]*pFunction, 0)
	graph.tails = make([]*pFunction, 0)
	graph.functions = make([]*pFunction, len(iR.Functions))
	for i, f := range iR.Functions {

		config := pFunctionConfig{
			&f,
			&graph.Stats.Functions[i],
			graph.channels,
		}
		function, err := buildPFunction(config)
		if err != nil {
			return Pipeline{}, err
		}

		if function.To == nil {
			graph.tails = append(graph.tails, function)
		}

		if function.From == nil {
			graph.roots = append(graph.roots, function)
		}

		if function.Identifier == iR.OnStartup {
			graph.onStartup = function
		}

		if function.Identifier == iR.OnTeardown {
			graph.onTeardown = function
		}

		graph.functions[i] = function
	}

	graph.flags.interactableCreated = false

	return graph, nil
}

////////////////////////////////////////////////////////////////////////
//						Pipeline functions
////////////////////////////////////////////////////////////////////////
//
//	Pipeline
//		∟ .Run( ... )
//		∟ .Test( data, ... )
//		∟ .TestAs( channel_name, data, ... )
//
////////////////////////////////////////////////////////////////////////

func (pipeline Pipeline) Interactable() Interactable {

	runtime := newPipelineRuntime(pipeline)
	interactable := Interactable{
		Id:       pDefaultId,
		Pipeline: pipeline.identifier,
		pipeline: pipeline,
		runtime:  runtime,
	}

	pipeline.flags.interactableCreated = true
	return interactable
}

func (pipeline Pipeline) Run(injectables ...any) error {

	if pipeline.flags.interactableCreated {
		return NonOwningPipeline
	}

	runtime := newPipelineRuntime(pipeline)
	runtime.injectDependencies(injectables...)

	return runtime.start()
}

type TestReport struct {
	Success bool   `json:"success"`
	Step    string `json:"step"`
	Cause   error  `json:"cause"`
}

func (pipeline Pipeline) Test(data any, injectables ...any) TestReport {

	if pipeline.flags.interactableCreated {
		return TestReport{
			Success: false,
			Cause:   NonOwningPipeline,
		}
	}

	runtime := newPipelineRuntime(pipeline)
	runtime.injectDependencies(injectables...)

	// instructs pipeline to enable testing
	runtime.isForTesting()

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		runtime.start()
		wg.Done()
	}()

	runtime.send(data)
	runtime.close()

	wg.Wait()

	return runtime.testingReport
}

func (pipeline Pipeline) TestAs(f string, data any, injectables ...any) TestReport {

	if pipeline.flags.interactableCreated {
		return TestReport{
			Success: false,
			Cause:   NonOwningPipeline,
		}
	}

	runtime := newPipelineRuntime(pipeline)
	runtime.injectDependencies(injectables...)

	// instructs pipeline to enable testing
	runtime.isForTesting()

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		runtime.start()
		wg.Done()
	}()

	runtime.send(data, f)
	runtime.close()

	wg.Wait()

	return runtime.testingReport
}
