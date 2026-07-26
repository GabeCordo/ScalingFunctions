// Package ScalingFunctions
//
// Copyright (c) 2024-2026. Gabriel Cordovado
// All rights reserved.
//
// Source file:  statistics.go
package ScalingFunctions

import (
	"fmt"
	"time"
)

type TimingStatistics struct {
	MinTimeBeforePop time.Duration `json:"min_time_before_pop_ns"`
	MaxTimeBeforePop time.Duration `json:"max_time_before_pop_ns"`
	AverageTime      time.Duration `json:"average_time_ns"`
	MedianTime       time.Duration `json:"median_time_ns"`
}

type FunctionStatistic struct {
	Active     uint16 `json:"active"`
	Provisions uint64 `json:"provisions"`
}

type PipeStatistic struct {
	Pushed   uint64
	Pulled   uint64           `json:"processed"`
	Dropped  uint64           `json:"dropped"`
	Breaches uint64           `json:"breaches"`
	Timing   TimingStatistics `json:"timing"`
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
		fmt.Printf("\tActive: %d\n\tProvisioned: %d\n", function.Active, function.Provisions)
	}

	for idx, pipe := range s.Pipes {
		fmt.Printf("Channel (%d):\n", idx)
		fmt.Printf("\tPushed: %d\n\tPulled: %d\n\tDropped: %d\n\tBreaches: %d\n", pipe.Pushed, pipe.Pulled, pipe.Dropped, pipe.Breaches)
		fmt.Println("\tTiming:")
		fmt.Printf("\t\tMin:%v\n\t\tMax:%v\n", pipe.Timing.MinTimeBeforePop, pipe.Timing.MaxTimeBeforePop)
	}
}

// NewStatistics
// is a constructor function to initialize memory for storing statistics.
func NewStatistics(numOfFunctions, numOfPipes int) *Statistics {
	stats := new(Statistics)

	stats.NumOfFunctions = 0
	stats.Functions = make([]FunctionStatistic, numOfFunctions)

	stats.NumOfChannels = 0
	stats.Pipes = make([]PipeStatistic, numOfPipes)

	return stats
}
