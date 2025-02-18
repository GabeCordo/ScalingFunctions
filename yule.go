// Package yule
//
// Copyright (c) 2024-2025. Gabriel Cordovado
// All rights reserved.
//
// Source file:  yule.go
package yule

import (
	"errors"
	"log"
	"reflect"
)

////////////////////////////////////////////////////////////////////////
//						  Yule Build Types
////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////
//					    Yule Build functions
////////////////////////////////////////////////////////////////////////

// build
// is a function to generate a RunnablePipeline instance from a set of
// one to many Branch instances.
func build(branches ...*Branch) (Pipeline, error) {

	iR, err := buildPipelineIR(branches...)
	if err != nil {
		return Pipeline{}, err
	}
	return buildPipeline(iR)
}

// buildLinear
// is a channelDataWrapper function to generate a branch from a set of functions. Instead
// of requiring the developer to write bloated code, the channelDataWrapper function can
// be used for simplistic functions that represent a line rather than tree.
func buildLinear(functions ...any) (Pipeline, error) {

	branch := NewBranch()
	for _, function := range functions {
		branch.Add(function)
	}
	return build(branch)
}

// buildFrom
// is a function to generate a runnable Pipeline from Pipeline metadata and repository.
func buildFrom(deployment *PipelineIR, repository *Repository) (Pipeline, error) {

	metadata, err := buildPipelineIRFrom(deployment, repository)
	if err != nil {
		return Pipeline{}, err
	}
	return buildPipeline(metadata)
}

// getBuildVariant
// is a function for parsing the parameters passed to Build and determining
// what sub-function needs to be called to generate the correct RunnablePipeline instance.
func getBuildVariant(inputs ...any) (variant buildVariant) {

	variant = invalidVariant

	for idx, input := range inputs {

		iType := invalidType

		// what type of value is 'i' that we received?
		if _, ok := input.(*Branch); ok {
			iType = branchType
		} else if _, ok = input.(*Repository); ok {
			iType = repositoryType
		} else if _, ok = input.(*PipelineIR); ok {
			iType = deploymentType
		} else if _, ok = input.(FunctionLink); ok {
			iType = functionWrapperType
		} else if reflect.TypeOf(input).Kind() == reflect.Func {
			iType = functionType
		} else {
			iType = invalidType
		}

		// cStatus machine determining whether the provided parameters are valid
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
					log.Panicf("the parameter at index %d is not a function channelDataWrapper\n", idx)
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
		default: // invalidVariant
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
				default: // invalidType
					log.Panicf("the parameter at index %d is unexpected\n", idx)
				}
			}
		}
	}

	return variant
}

////////////////////////////////////////////////////////////////////////
//						Yule Public functions
////////////////////////////////////////////////////////////////////////
//
//	Build( ... )
//		∟ Pipeline  : a runnable instance of the pipeline description
//			.Run()  : begins execution of the pipeline
//			.Test() : integration test on data passing through the pipeline
//
////////////////////////////////////////////////////////////////////////

// Build
// is a function to create a RunnablePipeline instance from a set of branches or functions.
//
// Variant 1:	Build(func1, func2, ... , funcn-1, funcn)
//
// Variant 2:   Build(branch1, branch2, ... , branchn-1, branchn)
//
// Variant 3:   Build(deployment, repository)
func Build(input ...any) (pipeline Pipeline) {

	numOfArguments := len(input)

	variant := getBuildVariant(input...)

	var err error = nil

	switch variant {
	case functionVariant, functionWrapperVariant:
		{
			pipeline, err = buildLinear(input...)
		}
	case branchVariant:
		{
			branches := make([]*Branch, numOfArguments)
			for i := 0; i < numOfArguments; i++ {
				branches[i] = (input[i]).(*Branch)
			}
			pipeline, err = build(branches...)
		}
	case configVariant:
		{
			d := (input[0]).(*PipelineIR)
			r := (input[1]).(*Repository)
			pipeline, err = buildFrom(d, r)
		}
	default:
		{
			err = errors.New("unknown build variant")
		}
	}

	if err != nil {
		panic(err)
	}

	return pipeline
}
