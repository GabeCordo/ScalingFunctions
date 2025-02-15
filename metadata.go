package yule

import (
	"errors"
	"fmt"
	"reflect"
)

const maximumGoroutinesPerFunction = 1000

// Metadata
// is a channelDataWrapper type containing a set of function pointers used in a
// Pipeline and a configuration description of the Pipeline.
type Metadata struct {
	Pipeline  PipelineMetadata
	Functions []FunctionLink
}

type FunctionLink struct {
	Id    string
	Max   int
	Value any
}

func buildMetadata(branches ...*Branch) (*Metadata, error) {

	// memory allocations //
	metadata := new(Metadata)

	metadata.Functions = make([]FunctionLink, 0)

	metadata.Pipeline.Pipes = make([]PipeMetadata, 0)
	metadata.Pipeline.Functions = make([]FunctionMetadata, 0)

	// default values //

	numOfPipes := 0

	functions := make(map[uintptr]FunctionMetadata)

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

			f := FunctionMetadata{StartWith: 1}
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
					p := PipeMetadata{Identifier: fmt.Sprint(numOfPipes), Threshold: 1, GrowthFactor: 2}
					metadata.Pipeline.Pipes = append(metadata.Pipeline.Pipes, p)

					// declare that the function pushes data to this pipe //
					f.To = fmt.Sprint(numOfPipes)

					// update the pipe records //
					previousPipe = fmt.Sprint(numOfPipes)
					numOfPipes++
				}
			}

			// add the built function //
			metadata.Pipeline.Functions = append(metadata.Pipeline.Functions, f)
			metadata.Functions = append(metadata.Functions, FunctionLink{Value: step.value, Id: step.id, Max: step.max})
			functions[sid] = f
		}

		previousPipe = ""
	}

	return metadata, nil
}

func buildMetadataFrom(deployment *PipelineMetadata, repository *Repository) (*Metadata, error) {

	if (deployment == nil) || (repository == nil) {
		return nil, errors.New("deployment or repository cannot be nil")
	}

	metadata := new(Metadata)
	metadata.Pipeline = *deployment
	metadata.Functions = make([]FunctionLink, 0)

	for _, function := range deployment.Functions {

		if m, found := repository.modules[function.Module]; found {

			if f, found := m.functions[function.Identifier]; found {
				metadata.Functions = append(metadata.Functions, FunctionLink{Value: f.Value})
			} else {
				// TODO : add more description
				panic("function module not found")
			}
		} else {
			// TODO : add more descriptions
			panic("module not found")
		}
	}

	return metadata, nil
}
