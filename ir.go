// Package ScalingFunctions
//
// Copyright (c) 2024-2026. Gabriel Cordovado
// All rights reserved.
//
// Source file:  ir.go
package ScalingFunctions

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
)

////////////////////////////////////////////////////////////////////////
//			Pipeline Intermediary Representation Constants
////////////////////////////////////////////////////////////////////////

const maximumGoroutinesPerFunction = 1000

////////////////////////////////////////////////////////////////////////
//			Pipeline Intermediary Representation Errors
////////////////////////////////////////////////////////////////////////

var UnsupportedIRVerifyType = errors.New("unsupported IR type for verification")
var EmptyIdentifier = errors.New("the identifier cannot be empty")
var EmptyVersion = errors.New("the module version cannot be empty")
var EmptyCreator = errors.New("the module creator cannot be empty")
var InvalidVersion = errors.New("the module version must either be of type '0.0.0' or 'v0.0.0'")
var NamingConflict = errors.New("the module contains functions with duplicate identifiers")
var UnsupportedIRCleanupType = errors.New("unsupported IR type for verification")

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

// versionType
// defines the format the version is in.
type versionType uint8

const (
	vPrefixedVersionType versionType = iota
	floatVersionType
	unknownVersionType
)

// irType
// defines what intermediary representation identity of a generic type.
type irType uint8

const (
	functionIRType irType = iota
	pipeIRType
	pipelineIRType
	moduleIRType
	unknownIRType
)

// FunctionMetadataIR
// is metadata associated with a FunctionIR
type FunctionMetadataIR struct {
	StaticMount bool `json:"static_mount" yaml:"static_mount"`
}

// FunctionIR
// is an intermediary representation of a function.
type FunctionIR struct {
	Module     string             `json:"module"`
	Identifier string             `json:"id" yaml:"id"`
	Metadata   FunctionMetadataIR `json:"metadata" yaml:"metadata"`
	From       string             `json:"from,omitempty" yaml:"from,omitempty"`             // what pipe a function is sending data to.
	To         string             `json:"to,omitempty" yaml:"to,omitempty"`                 // which pipe the function is receiving data from.
	StartWith  uint16             `json:"start_with,omitempty" yaml:"start_with,omitempty"` // start with N instances of the function running in parallel.
	WaitBefore bool               `json:"wait_before,omitempty" yaml:"wait_before,omitempty"`
	Maximum    uint16             `json:"maximum,omitempty" yaml:"maximum,omitempty"` // maximum instances of the function that can be run at the same time
	Parameters []string           `json:"parameters" yaml:"parameters"`
	Returns    []string           `json:"returns" yaml:"returns"`
	value      any
}

// PipeIR
// is an intermediary representation of a pipe.
type PipeIR struct {
	Identifier   string  `json:"id" yaml:"id"`
	Threshold    uint32  `json:"threshold" yaml:"threshold"`         // the amount of data that must sit idle in a pipe before we consider the pipe congested.
	GrowthFactor float64 `json:"growth_factor" yaml:"growth_factor"` // the factor the number of receivers will multiply by to reduce congestion in the pipe.
}

// PipelineIR
// is an intermediary representation of a pipeline.
type PipelineIR struct {
	Identifier string       `json:"id" yaml:"id"`
	OnCrash    OnCrash      `json:"on_crash,omitempty" yaml:"on_crash,omitempty"`
	Functions  []FunctionIR `json:"functions" yaml:"functions"`
	Pipes      []PipeIR     `json:"pipes" yaml:"pipes"`
	OnStartup  string       `json:"on_startup" yaml:"on_startup,omitempty"`
	OnTeardown string       `json:"on_teardown" yaml:"on_teardown,omitempty"`
}

// ContactIR
// is used to attach contact information to a ModuleIR
type ContactIR struct {
	Name  string `json:"name" yaml:"name"`
	Email string `json:"email" yaml:"email"`
}

// ModuleIR
// is an intermediary representation of a module.
type ModuleIR struct {
	Identifier string       `json:"id" yaml:"id"`
	Version    string       `json:"version" yaml:"version"`
	Contact    ContactIR    `json:"contact" yaml:"contact"`
	Functions  []FunctionIR `json:"functions" yaml:"functions"`
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

	for idx := range deployment.Functions {

		m, moduleFound := repository.modules[deployment.Functions[idx].Module]
		if !moduleFound {
			// TODO : add more descriptions
			panic("module not found")
		}

		f, functionFound := m.functions[deployment.Functions[idx].Identifier]
		if !functionFound {
			// TODO : add more description
			panic("function module not found")
		}

		deployment.Functions[idx].value = f.Value
		if f.Max > 0 {
			deployment.Functions[idx].Maximum = f.Max
		}
	}

	return deployment, nil
}

////////////////////////////////////////////////////////////////////////
//	     Exported Pipeline Intermediary Representation Functions
////////////////////////////////////////////////////////////////////////

func findVersionType(version string) versionType {

	if len(version) < 1 {
		return unknownVersionType
	} else if version[0] == 'v' {
		return vPrefixedVersionType
	} else {
		return floatVersionType
	}
}

func verifyVersionInModule(version string) (err error) {

	vType := findVersionType(version)

	switch vType {
	case vPrefixedVersionType:
		{
			version = version[1 : len(version)-1]
			_, e := strconv.ParseFloat(version, 64)
			if e != nil {
				err = InvalidVersion
			}
		}
	case floatVersionType:
		{
			_, e := strconv.ParseFloat(version, 64)
			if e != nil {
				err = InvalidVersion
			}
		}
	default:
		{
			err = EmptyVersion
		}
	}

	return err
}

func verifyModuleIR(ir *ModuleIR) (ee []error) {

	ee = make([]error, 0)

	if ir.Identifier == "" {
		ee = append(ee, EmptyIdentifier)
	}

	err := verifyVersionInModule(ir.Version)
	if err != nil {
		ee = append(ee, err)
	}

	// ensure that every export identifier is unique
	exports := make(map[string]bool)

	for _, function := range ir.Functions {
		// pull the unique identifier inside the function struct
		id := function.Identifier

		// see if the unique identifier already exists inside the module
		if _, found := exports[id]; found {
			// when there is a unique identifier clash, the module is no longer valid
			ee = append(ee, NamingConflict)
		} else {
			// otherwise, add to the map so that it is easier to detect clashes
			exports[id] = true
		}
	}

	return ee
}

func getIRType(ir any) (t irType) {

	var ok bool
	if _, ok = ir.(*ModuleIR); ok {
		t = moduleIRType
	} else if _, ok = ir.(*PipelineIR); ok {
		t = pipelineIRType
	} else if _, ok = ir.(*FunctionIR); ok {
		t = functionIRType
	} else if _, ok = ir.(*PipeIR); ok {
		t = pipeIRType
	} else {
		t = unknownIRType
	}

	return t
}

func VerifyIR(ir any) (errors []error) {

	errors = make([]error, 0)

	switch getIRType(ir) {
	case moduleIRType:
		{
			moduleIR := ir.(*ModuleIR)
			errors = append(errors, verifyModuleIR(moduleIR)...)
		}
	default:
		{
			// the function shall return 'false' when not implemented for other type
			errors = append(errors, UnsupportedIRVerifyType)
		}
	}

	return errors
}

func cleanupFunctionIR(functionIR *FunctionIR) {

	if functionIR.Maximum == 0 {
		functionIR.Maximum = 1
	}

	if functionIR.StartWith == 0 {
		functionIR.StartWith = 1
	}
}

func cleanupModuleIR(moduleIR *ModuleIR) {

	err := verifyVersionInModule(moduleIR.Version)
	if errors.Is(err, EmptyVersion) {
		moduleIR.Version = "v0.0.1"
	}

	for idx := range moduleIR.Functions {
		cleanupFunctionIR(&moduleIR.Functions[idx])
	}
}

func cleanupPipelineIR(pipelineIR *PipelineIR) {

	for idx := range pipelineIR.Functions {
		cleanupFunctionIR(&pipelineIR.Functions[idx])
	}
}

// CleanupIR
// is a function to fix erroneous or malicious modifications to intermediate
// representations that are received from external sources.
func CleanupIR(ir any) error {

	switch getIRType(ir) {
	case moduleIRType:
		{
			moduleIR := ir.(*ModuleIR)
			cleanupModuleIR(moduleIR)
		}
	case pipelineIRType:
		{
			pipelineIR := ir.(*PipelineIR)
			cleanupPipelineIR(pipelineIR)
		}
	default:
		{
			return UnsupportedIRCleanupType
		}
	}

	return nil
}
