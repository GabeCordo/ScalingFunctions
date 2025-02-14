package yule

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
	Active     int `json:"active"`
	Provisions int `json:"provisions"`
}

type PipeStatistic struct {
	Pushed   int
	Pulled   int              `json:"processed"`
	Dropped  int              `json:"dropped"`
	Breaches int              `json:"breaches"`
	Timing   TimingStatistics `json:"timing"`
}

type Statistics struct {
	NumOfFunctions int                 `json:"num_of_functions"`
	Functions      []FunctionStatistic `json:"steps"`
	NumOfChannels  int                 `json:"num_of_channels"`
	Pipes          []PipeStatistic     `json:"channels"`
}

// Print
// is a debug function used to view a snapshot of the Pipeline at an instance in time.
func (s Statistics) Print() {

	for idx, function := range s.Functions {
		fmt.Printf("FunctionMetadata (%d):\n", idx)
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
