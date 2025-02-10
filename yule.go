package yule

import (
	"errors"
	"log"
	"reflect"
)

// build
// is a function to generate a Runnable instance from a set of
// one to many Branch instances.
func build(branches ...*Branch) (*Runnable, error) {

	metadata, err := buildMetadata(branches...)
	if err != nil {
		return nil, err
	}
	return newRunnable(metadata)
}

// buildLinear
// is a wrapper function to generate a branch from a set of functions. Instead
// of requiring the developer to write bloated code, the wrapper function can
// be used for simplistic functions that represent a line rather than tree.
func buildLinear(functions ...any) (*Runnable, error) {

	branch := NewBranch()
	for _, function := range functions {
		branch.Add(function)
	}
	return build(branch)
}

func buildFrom(deployment *Deployment, repository *Repository) (*Runnable, error) {

	metadata, err := buildMetadataFrom(deployment, repository)
	if err != nil {
		return nil, err
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

	var err error = nil

	switch variant {
	case functionVariant, functionWrapperVariant:
		{
			runnable, err = buildLinear(input...)
		}
	case branchVariant:
		{
			branches := make([]*Branch, numOfArguments)
			for i := 0; i < numOfArguments; i++ {
				branches[i] = (input[i]).(*Branch)
			}
			runnable, err = build(branches...)
		}
	case configVariant:
		{
			d := (input[0]).(*Deployment)
			r := (input[1]).(*Repository)
			runnable, err = buildFrom(d, r)
		}
	default:
		{
			err = errors.New("unknown build variant")
		}
	}

	if err != nil {
		panic(err)
	}

	return runnable
}
