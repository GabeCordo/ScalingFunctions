package yule

import (
	"fmt"
	"log"
	"reflect"
)

type F struct {
	Id    string
	Value any
}

// Metadata
// is a wrapper type containing a set of function pointers used in a
// pipeline and a configuration description of the pipeline.
type Metadata struct {
	Deployment *Deployment
	Functions  []F
}

// build
// is a function to generate a Runnable instance from a set of
// one to many Branch instances.
func build(branches ...*Branch) *Runnable {

	// memory allocations //
	metadata := new(Metadata)

	deployment := new(Deployment)
	metadata.Deployment = deployment
	metadata.Functions = make([]F, 0)

	deployment.Pipes = make([]Pipe, 0)
	deployment.Functions = make([]Function, 0)

	// default values //

	numOfPipes := 0

	functions := make(map[uintptr]Function)

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
				panic("repeated function must be at the end of the branch")
			}

			f := Function{StartWith: 1}
			if step.id != "" {
				f.Identifier = step.id
			} else {
				step.id = fmt.Sprint(sid)
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
					p := Pipe{Identifier: fmt.Sprint(numOfPipes), Threshold: 1, GrowthFactor: 2}
					deployment.Pipes = append(deployment.Pipes, p)

					// declare that the function pushes data to this pipe //
					f.To = fmt.Sprint(numOfPipes)

					// update the pipe records //
					previousPipe = fmt.Sprint(numOfPipes)
					numOfPipes++
				}
			}

			// add the built function //
			deployment.Functions = append(deployment.Functions, f)
			metadata.Functions = append(metadata.Functions, F{Value: step.value, Id: step.id})
			functions[sid] = f
		}

		previousPipe = ""
	}

	return newRunnable(metadata)
}

// buildLinear
// is a wrapper function to generate a branch from a set of functions. Instead
// of requiring the developer to write bloated code, the wrapper function can
// be used for simplistic functions that represent a line rather than tree.
func buildLinear(functions ...any) *Runnable {

	branch := NewBranch()
	for _, function := range functions {
		branch.Add(function)
	}
	return build(branch)
}

func buildFrom(deployment *Deployment, repository *Repository) *Runnable {

	if deployment == nil {
		panic("deployment cannot be nil")
	}

	metadata := new(Metadata)
	metadata.Deployment = deployment
	metadata.Functions = make([]F, 0)

	for _, function := range deployment.Functions {

		if m, found := repository.modules[function.Module]; found {

			if f, found := m.functions[function.Identifier]; found {
				metadata.Functions = append(metadata.Functions, F{Value: f.Value})
			} else {
				// TODO : add more description
				panic("function module not found")
			}
		} else {
			// TODO : add more descriptions
			panic("module not found")
		}
	}

	return newRunnable(metadata)
}

// notableType
// categorises and input to a lexeme.
type notableType uint8

const (
	functionType notableType = iota
	functionWrapperType
	branchType
	deploymentType
	repositoryType
	invalidType
)

// buildVariant
// describes how the Build function behaves to a set of inputs.
type buildVariant uint8

const (
	functionVariant buildVariant = iota
	functionWrapperVariant
	branchVariant
	configVariant
	invalidVariant
)

// getBuildVariant
// is a function for parsing the parameters passed to Build and determining
// what sub-function needs to be called to generate the correct Runnable instance.
func getBuildVariant(inputs ...any) (variant buildVariant) {

	variant = invalidVariant

	for idx, input := range inputs {

		iType := invalidType

		// what type of value is 'i' that we received?
		if _, ok := input.(*Branch); ok {
			iType = branchType
		} else if _, ok = input.(*Repository); ok {
			iType = repositoryType
		} else if _, ok = input.(*Deployment); ok {
			iType = deploymentType
		} else if _, ok = input.(F); ok {
			iType = functionWrapperType
		} else if reflect.TypeOf(input).Kind() == reflect.Func {
			iType = functionType
		} else {
			iType = invalidType
		}

		// state machine determining whether the provided parameters are valid
		switch variant {
		case functionVariant:
			{
				if iType != functionType {
					log.Panicf("the parameter at index %d is not a function\n", idx)
				}
			}
		case functionWrapperVariant:
			{
				if iType != functionWrapperType {
					log.Panicf("the parameter at index %d is not a function wrapper\n", idx)
				}
			}
		case branchVariant:
			{
				if iType != branchType {
					log.Panicf("the parameter at index %d is not a branch\n", idx)
				}
			}
		case configVariant:
			{
				if iType != repositoryType {
					log.Panicf("the parameter at index %d is not a repository\n", idx)
				}
			}
		case invalidVariant:
			{
				switch iType {
				case branchType:
					variant = branchVariant
				case functionType:
					variant = functionVariant
				case functionWrapperType:
					variant = functionWrapperVariant
				case deploymentType:
					variant = configVariant
				case repositoryType:
					log.Panicf("the parameter at index %d is a repository; did you mean to provide a deployment first?\n", idx)
				case invalidType:
					log.Panicf("the parameter at index %d is unexpected\n", idx)
				}
			}
		}
	}

	return variant
}

// Build
// is a function to create a Runnable instance from a set of branches or functions.
//
// Variant 1:	Build(func1, func2, ... , funcn-1, funcn)
//
// Variant 2:   Build(branch1, branch2, ... , branchn-1, branchn)
//
// Variant 3:   Build(deployment, repository)
func Build(input ...any) (runnable *Runnable) {

	numOfArguments := len(input)

	variant := getBuildVariant(input...)

	switch variant {
	case functionVariant, functionWrapperVariant:
		{
			runnable = buildLinear(input...)
		}
	case branchVariant:
		{
			branches := make([]*Branch, numOfArguments)
			for i := 0; i < numOfArguments; i++ {
				branches[i] = (input[i]).(*Branch)
			}
			runnable = build(branches...)
		}
	case configVariant:
		{
			d := (input[0]).(*Deployment)
			r := (input[1]).(*Repository)
			runnable = buildFrom(d, r)
		}
	default:
		{
			panic("unknown build variant")
		}
	}

	return runnable
}
