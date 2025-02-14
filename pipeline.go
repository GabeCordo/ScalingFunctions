// Package yule
//
// Copyright (c) 2024-2025. Gabriel Cordovado
// All rights reserved.
package yule

import (
	"reflect"
	"sync"
)

// pChannelConfig
// holds data required to build a pChannel.
type pChannelConfig struct {
	p *PipeMetadata
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
		Threshold    int
	} `json:"-"`

	Mutex sync.RWMutex `json:"-"`
}

// pFunctionConfig
// holds data required to build a pFunction.
type pFunctionConfig struct {
	f        *FunctionMetadata
	m        *FunctionLink
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
		StartWith  int
		WaitBefore bool
		Maximum    int
	} `json:"-"`

	Mutex sync.RWMutex `json:"-"`
}

// Pipeline
// represents a graph of pFunction and pChannel linked together.
type Pipeline struct {
	Identifier string

	Roots []*pFunction `json:"-"`
	Tails []*pFunction

	Channels  []*pChannel  `json:"-"`
	Functions []*pFunction `json:"-"`

	OnStartup  *pFunction
	OnTeardown *pFunction

	Stats *Statistics
}

func buildPChannel(config pChannelConfig) (*pChannel, error) {

	newChan := new(pChannel)

	newChan.Identifier = config.p.Identifier
	newChan.Config.Threshold = config.p.Threshold
	newChan.Config.GrowthFactor = config.p.GrowthFactor
	newChan.Stats = config.s
	newChan.Value = New(newChan.Identifier, config.p.Threshold, config.p.GrowthFactor, &config.s.Timing)
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
	function.Value = config.m.Value
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

func buildPipeline(metadata *Metadata) (Pipeline, error) {

	graph := Pipeline{}
	graph.Identifier = metadata.Pipeline.Identifier

	// todo : this mem allocation should not be here
	graph.Stats = NewStatistics(len(metadata.Pipeline.Functions), len(metadata.Pipeline.Pipes))

	graph.Channels = make([]*pChannel, len(metadata.Pipeline.Pipes))
	for i, c := range metadata.Pipeline.Pipes {
		config := pChannelConfig{
			p: &c,
			s: &graph.Stats.Pipes[i],
		}
		newChan, err := buildPChannel(config)
		if err != nil {
			return Pipeline{}, err
		}
		graph.Channels[i] = newChan
	}

	graph.Roots = make([]*pFunction, 0)
	graph.Tails = make([]*pFunction, 0)
	graph.Functions = make([]*pFunction, len(metadata.Pipeline.Functions))
	for i, f := range metadata.Pipeline.Functions {

		config := pFunctionConfig{
			&f,
			&metadata.Functions[i],
			&graph.Stats.Functions[i],
			graph.Channels,
		}
		function, err := buildPFunction(config)
		if err != nil {
			return Pipeline{}, err
		}

		if function.To == nil {
			graph.Tails = append(graph.Tails, function)
		}

		if function.From == nil {
			graph.Roots = append(graph.Roots, function)
		}

		if function.Identifier == metadata.Pipeline.OnStartup {
			graph.OnStartup = function
		}

		if function.Identifier == metadata.Pipeline.OnTeardown {
			graph.OnTeardown = function
		}

		graph.Functions[i] = function
	}

	return graph, nil
}
