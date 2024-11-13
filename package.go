package yule

import (
	"fmt"
	"reflect"
)

type F struct {
	name  string
	value any
}

type Metadata struct {
	Deployment *Deployment
	Functions  []F
}

// A1 ---> B -> C
// A2 _/

// let pipeline~
//
//	  b1 := yule.Branch(a).SendTo(b).SendTo(c)
//	  b2 := yule.Branch(a2).SendTo(b)
//
//    p := yule.Pipeline(b1, b2)

func Build(branches ...*Branch) *Metadata {

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

			f := Function{Identifier: fmt.Sprint(sid), StartWith: 1}
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
			metadata.Functions = append(metadata.Functions, F{value: step.value})
			functions[sid] = f
		}

		previousPipe = ""
	}

	return metadata
}

func BuildFrom(deployment *Deployment, repository *Repository) *Metadata {

	if deployment == nil {
		panic("deployment cannot be nil")
	}

	metadata := new(Metadata)
	metadata.Deployment = deployment
	metadata.Functions = make([]F, 0)

	for _, function := range deployment.Functions {

		if m, found := repository.modules[function.Module]; found {

			if f, found := m.functions[function.Identifier]; found {
				metadata.Functions = append(metadata.Functions, F{value: f.value})
			} else {
				// TODO : add more description
				panic("function module not found")
			}
		} else {
			// TODO : add more descriptions
			panic("module not found")
		}
	}

	return metadata
}
