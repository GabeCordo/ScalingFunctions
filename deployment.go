package yule

// OnCrash
// describes what should be done when a function fails.
type OnCrash string

const (
	Restart   OnCrash = "Restart"
	DoNothing         = "DoNothing"
)

// OnLoad
// describes what a function should do before accepting channel data.
type OnLoad string

const (
	CompleteAndPush OnLoad = "CompleteAndPush" // tells a function to immediately process data.
	WaitAndPush            = "WaitAndPush"     // tells a function to wait for a channel to close before processing data as an array.
)

type Function struct {
	Module     string `json:"module"`
	Identifier string `json:"id" yaml:"id"`
	From       string `json:"from,omitempty" yaml:"from,omitempty"`             // what pipe a function is sending data to.
	To         string `json:"to,omitempty" yaml:"to,omitempty"`                 // which pipe the function is receiving data from.
	StartWith  int    `json:"start_with,omitempty" yaml:"start_with,omitempty"` // start with N instances of the function running in parallel.
	WaitBefore bool   `json:"wait_before,omitempty" yaml:"wait_before"`
}

type Pipe struct {
	Identifier   string  `json:"id" yaml:"id"`
	Threshold    int     `json:"threshold yaml:"threshold""`         // the amount of data that must sit idle in a pipe before we consider the pipe congested.
	GrowthFactor float64 `json:"growth_factor" yaml:"growth_factor"` // the factor the number of receivers will multiply by to reduce congestion in the pipe.
}

type Deployment struct {
	Identifier string     `json:"id" yaml:"id"`
	OnCrash    OnCrash    `json:"on_crash,omitempty" yaml:"on_crash,omitempty"`
	Functions  []Function `json:"functions" yaml:"functions"`
	Pipes      []Pipe     `json:"pipes" yaml:"pipes"`
	OnStartup  string     `json:"on_startup" yaml:"on_startup,omitempty"`
	OnTeardown string     `json:"on_teardown" yaml:"on_teardown,omitempty"`
}
