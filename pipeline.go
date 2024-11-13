package yule

import (
	"sync"
)

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

type pFunction struct {
	Identifier string
	To         *pChannel
	From       *pChannel
	Stats      *FunctionStatistic `json:"-"`
	Quit       []chan bool        `json:"-"`
	Value      any                `json:"-"`

	Config struct {
		StartWith  int
		WaitBefore bool
	} `json:"-"`

	Mutex sync.RWMutex `json:"-"`
}

func (function *pFunction) Next(foo any) *pFunction {

	f := new(pFunction)
	f.Value = foo

	return f
}

type Pipeline struct {
	Identifier string

	Roots []*pFunction `json:"-"`
	Tails []*pFunction

	Channels  []*pChannel  `json:"-"`
	Functions []*pFunction `json:"-"`

	OnStartup  *pFunction
	OnTeardown *pFunction

	Stats *Statistics

	Mutex sync.RWMutex `json:"-"`
}
