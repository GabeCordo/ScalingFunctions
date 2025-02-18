// Package yule
//
// Copyright (c) 2024-2025. Gabriel Cordovado
// All rights reserved.
//
// Source file:  pipeline_intermediary_representation.go
package yule

import (
	"errors"
	"fmt"
	"reflect"
)

////////////////////////////////////////////////////////////////////////
//			Pipeline Intermediary Representation Constants
////////////////////////////////////////////////////////////////////////

const maximumGoroutinesPerFunction = 1000

////////////////////////////////////////////////////////////////////////
//			Pipeline Intermediary Representation Types
////////////////////////////////////////////////////////////////////////

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

type FunctionIR struct {
	Module     string `json:"module"`
	Identifier string `json:"id" yaml:"id"`
	From       string `json:"from,omitempty" yaml:"from,omitempty"`             // what pipe a function is sending data to.
	To         string `json:"to,omitempty" yaml:"to,omitempty"`                 // which pipe the function is receiving data from.
	StartWith  int    `json:"start_with,omitempty" yaml:"start_with,omitempty"` // start with N instances of the function running in parallel.
	WaitBefore bool   `json:"wait_before,omitempty" yaml:"wait_before"`
	Maximum    int    `json:"maximum,omitempty" yaml:"maximum"` // maximum instances of the function that can be run at the same time
	value      any
}

type PipeIR struct {
	Identifier   string  `json:"id" yaml:"id"`
	Threshold    int     `json:"threshold yaml:"threshold""`         // the amount of data that must sit idle in a pipe before we consider the pipe congested.
	GrowthFactor float64 `json:"growth_factor" yaml:"growth_factor"` // the factor the number of receivers will multiply by to reduce congestion in the pipe.
}

type PipelineIR struct {
	Identifier string       `json:"id" yaml:"id"`
	OnCrash    OnCrash      `json:"on_crash,omitempty" yaml:"on_crash,omitempty"`
	Functions  []FunctionIR `json:"functions" yaml:"functions"`
	Pipes      []PipeIR     `json:"pipes" yaml:"pipes"`
	OnStartup  string       `json:"on_startup" yaml:"on_startup,omitempty"`
	OnTeardown string       `json:"on_teardown" yaml:"on_teardown,omitempty"`
}

////////////////////////////////////////////////////////////////////////
//			Pipeline Intermediary Representation Functions
////////////////////////////////////////////////////////////////////////

func buildPipelineIR(branches ...*Branch) (*PipelineIR, error) {

	// memory allocations //
	iR := new(PipelineIR)

	iR.Pipes = make([]PipeIR, 0)
	iR.Functions = make([]FunctionIR, 0)

	// default values //

	numOfPipes := 0

	functions := make(map[uintptr]FunctionIR)

	// each branch describes a sequential chain of functions
	for _, branch := range branches {

		numOfSteps := len(branch.steps)

		previousPipe := ""

		// the functions inside each branch can point to other branches
		for jdx, step := range branch.steps {

			isLastStep := jdx == (numOfSteps - 1)

			sid := reflect.ValueOf(step.value).Pointer()

			// is the step function already found?
			if _, found := functions[sid]; found && isLastStep {
				continue
			} else if found && !isLastStep {
				return nil, errors.New("repeated function must be at the end of the branch")
			}

			f := FunctionIR{StartWith: 1, value: step.value}
			if step.id != "" {
				f.Identifier = step.id
			} else {
				step.id = fmt.Sprint(sid)
				f.Identifier = step.id
			}
			if (step.max >= 1) && (step.max <= maximumGoroutinesPerFunction) {
				f.Maximum = step.max
			} else {
				f.Maximum = maximumGoroutinesPerFunction
			}
			f.From = previousPipe

			// if the function is not a terminal step we will need to create a channel
			// to send data to.
			if !isLastStep {

				// check if the next step points to a known branch
				nextSid := reflect.ValueOf(branch.steps[jdx+1].value).Pointer()

				if repeatedFunc, found := functions[nextSid]; found {
					// point to an existing pipe
					f.To = repeatedFunc.From
				} else {
					// create a new pipe the function pushes data to //
					p := PipeIR{Identifier: fmt.Sprint(numOfPipes), Threshold: 1, GrowthFactor: 2}
					iR.Pipes = append(iR.Pipes, p)

					// declare that the function pushes data to this pipe //
					f.To = fmt.Sprint(numOfPipes)

					// update the pipe records //
					previousPipe = fmt.Sprint(numOfPipes)
					numOfPipes++
				}
			}

			// add the built function //
			iR.Functions = append(iR.Functions, f)
			functions[sid] = f
		}

		previousPipe = ""
	}

	return iR, nil
}

func buildPipelineIRFrom(deployment *PipelineIR, repository *Repository) (*PipelineIR, error) {

	if (deployment == nil) || (repository == nil) {
		return nil, errors.New("deployment or repository cannot be nil")
	}

	for _, function := range deployment.Functions {

		if m, found := repository.modules[function.Module]; found {

			if f, found := m.functions[function.Identifier]; found {
				function.value = f
			} else {
				// TODO : add more description
				panic("function module not found")
			}
		} else {
			// TODO : add more descriptions
			panic("module not found")
		}
	}

	return deployment, nil
}
