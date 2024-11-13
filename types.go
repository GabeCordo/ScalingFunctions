package yule

import "time"

type OnCrash string

const (
	Restart   OnCrash = "Restart"
	DoNothing         = "DoNothing"
)

type OnLoad string

const (
	CompleteAndPush OnLoad = "CompleteAndPush"
	WaitAndPush            = "WaitAndPush"
)

type RunMode string

const (
	Batch  RunMode = "Batch"
	Stream         = "Stream"
)

type Function struct {
	Module     string `json:"module"`
	Identifier string `json:"id" yaml:"id"`
	From       string `json:"from,omitempty" yaml:"from,omitempty"`
	To         string `json:"to,omitempty" yaml:"to,omitempty"`
	StartWith  int    `json:"start_with,omitempty" yaml:"start_with,omitempty"`
	WaitBefore bool   `json:"wait_before,omitempty" yaml:"wait_before"`
}

type Pipe struct {
	Identifier   string  `json:"id" yaml:"id"`
	Threshold    int     `json:"threshold yaml:"threshold""`
	GrowthFactor float64 `json:"growth_factor" yaml:"growth_factor"`
}

type Deployment struct {
	Identifier string     `json:"id" yaml:"id"`
	OnCrash    OnCrash    `json:"on_crash,omitempty" yaml:"on_crash,omitempty"`
	Functions  []Function `json:"functions" yaml:"functions"`
	Pipes      []Pipe     `json:"pipes" yaml:"pipes"`
	OnStartup  string     `json:"on_startup" yaml:"on_startup,omitempty"`
	OnTeardown string     `json:"on_teardown" yaml:"on_teardown,omitempty"`
}

type DataTiming struct {
	ETIn  time.Time
	ETOut time.Time
	TLIn  time.Time
	TLOut time.Time
}

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

func NewStatistics(numOfFunctions, numOfPipes int) *Statistics {
	stats := new(Statistics)

	stats.NumOfFunctions = 0
	stats.Functions = make([]FunctionStatistic, numOfFunctions)

	stats.NumOfChannels = 0
	stats.Pipes = make([]PipeStatistic, numOfPipes)

	return stats
}

type DataTimer struct {
	In  time.Time
	Out time.Time
}

// TODO : move out of here to somewhere ELSE!

func (dataTiming DataTiming) Valid() bool {
	return !dataTiming.ETIn.IsZero() && !dataTiming.ETOut.IsZero() && !dataTiming.TLIn.IsZero() && !dataTiming.TLOut.IsZero()
}
