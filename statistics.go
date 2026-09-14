// Package ScalingFunctions
//
// Copyright (c) 2024-2026. Gabriel Cordovado
// All rights reserved.
//
// Source file:  statistics.go
package ScalingFunctions

import (
	"fmt"
	"sync/atomic"
	"time"
)

type TimingStatistics struct {
	MinTimeBeforePop time.Duration `json:"min_time_before_pop_ns"`
	MaxTimeBeforePop time.Duration `json:"max_time_before_pop_ns"`
	AverageTime      time.Duration `json:"average_time_ns"`
	MedianTime       time.Duration `json:"median_time_ns"`
}

type FunctionStatistic struct {
	Active     uint32 `json:"active"`
	Provisions uint64 `json:"provisions"`
}

func (s *FunctionStatistic) AddActive(delta int32) {
	atomic.AddUint32(&s.Active, uint32(delta))
}

func (s *FunctionStatistic) GetActive() uint32 {
	return atomic.LoadUint32(&s.Active)
}

func (s *FunctionStatistic) IncProvisions() {
	atomic.AddUint64(&s.Provisions, 1)
}

func (s *FunctionStatistic) GetProvisions() uint64 {
	return atomic.LoadUint64(&s.Provisions)
}

type PipeStatistic struct {
	Pushed   uint64
	Pulled   uint64           `json:"processed"`
	Dropped  uint64           `json:"dropped"`
	Breaches uint64           `json:"breaches"`
	Timing   TimingStatistics `json:"timing"`
}

func (s *PipeStatistic) IncPushed(n uint64) {
	atomic.AddUint64(&s.Pushed, n)
}

func (s *PipeStatistic) GetPushed() uint64 {
	return atomic.LoadUint64(&s.Pushed)
}

func (s *PipeStatistic) IncPulled() {
	atomic.AddUint64(&s.Pulled, 1)
}

func (s *PipeStatistic) GetPulled() uint64 {
	return atomic.LoadUint64(&s.Pulled)
}

func (s *PipeStatistic) IncDropped() {
	atomic.AddUint64(&s.Dropped, 1)
}

func (s *PipeStatistic) GetDropped() uint64 {
	return atomic.LoadUint64(&s.Dropped)
}

func (s *PipeStatistic) IncBreaches() {
	atomic.AddUint64(&s.Breaches, 1)
}

func (s *PipeStatistic) GetBreaches() uint64 {
	return atomic.LoadUint64(&s.Breaches)
}

type Statistics struct {
	NumOfFunctions uint16              `json:"num_of_functions"`
	Functions      []FunctionStatistic `json:"steps"`
	NumOfChannels  uint16              `json:"num_of_channels"`
	Pipes          []PipeStatistic     `json:"channels"`
}

// Print
// is a debug function used to view a snapshot of the Pipeline at an instance in time.
func (s Statistics) Print() {

	for idx, function := range s.Functions {
		fmt.Printf("FunctionIR (%d):\n", idx)
		fmt.Printf("\tActive: %d\n\tProvisioned: %d\n", atomic.LoadUint32(&function.Active), atomic.LoadUint64(&function.Provisions))
	}

	for idx, pipe := range s.Pipes {
		fmt.Printf("Channel (%d):\n", idx)
		fmt.Printf("\tPushed: %d\n\tPulled: %d\n\tDropped: %d\n\tBreaches: %d\n", atomic.LoadUint64(&pipe.Pushed), atomic.LoadUint64(&pipe.Pulled), atomic.LoadUint64(&pipe.Dropped), atomic.LoadUint64(&pipe.Breaches))
		fmt.Println("\tTiming:")
		fmt.Printf("\t\tMin:%v\n\t\tMax:%v\n", pipe.Timing.MinTimeBeforePop, pipe.Timing.MaxTimeBeforePop)
	}
}

// NewStatistics
// is a constructor function to initialize memory for storing statistics.
func NewStatistics(numOfFunctions, numOfPipes uint16) *Statistics {
	stats := new(Statistics)

	stats.NumOfFunctions = numOfFunctions
	stats.Functions = make([]FunctionStatistic, numOfFunctions)

	stats.NumOfChannels = numOfPipes
	stats.Pipes = make([]PipeStatistic, numOfPipes)

	return stats
}
