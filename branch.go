// Package ScalingFunctions
//
// Copyright (c) 2024-2025. Gabriel Cordovado
// All rights reserved.
//
// Source file:  branch.go
package ScalingFunctions

import (
	"errors"
	"reflect"
)

////////////////////////////////////////////////////////////////////////
//						Branch Constants
////////////////////////////////////////////////////////////////////////

// defaultStaticMount
// is the default metadata mount type for a function.
const defaultStaticMount = true

////////////////////////////////////////////////////////////////////////
//						Branch Errors
////////////////////////////////////////////////////////////////////////

var IsNotFunc = errors.New("passed value must be of type function")

////////////////////////////////////////////////////////////////////////
//						Branch Types
////////////////////////////////////////////////////////////////////////

// Step
// is a channelDataWrapper to associate a function to a string identifier.
type Step struct {
	id    string
	value any
	max   uint16
}

// Branch
// is a series of functions chained one after another.
type Branch struct {
	steps []Step
}

// M
// is a collection of metadata that is bound to a function.
type M struct {
	id string
}

// F
// is metadata tightly coupled with a function.
type F struct {
	Id    string
	Max   uint16
	Value any
}

////////////////////////////////////////////////////////////////////////
//						Branch Functions
////////////////////////////////////////////////////////////////////////

// GetIR
// returns the intermidary representation of the `F` object.
func (f F) GetIR() FunctionIR {

	ir := FunctionIR{}
	ir.Identifier = f.Id
	ir.Metadata.StaticMount = defaultStaticMount
	ir.Maximum = f.Max

	reflected := reflect.TypeOf(f.Value)

	numOfParameters := reflected.NumIn()
	ir.Parameters = make([]string, numOfParameters)
	for i := 0; i < numOfParameters; i++ {
		ir.Parameters[i] = reflected.In(i).String()
	}

	numOfReturns := reflected.NumOut()
	ir.Returns = make([]string, numOfReturns)
	for i := 0; i < numOfReturns; i++ {
		ir.Returns[i] = reflected.Out(i).String()
	}

	return ir
}

// Add
// allows the developer to define new function steps using the builder pattern.
func (b *Branch) Add(data any) *Branch {

	var v reflect.Value
	var k reflect.Kind

	var s Step

	if w, isWrapperType := data.(F); isWrapperType {
		v = reflect.ValueOf(w.Value)
		k = v.Kind()

		s.id = w.Id
		s.value = w.Value
		s.max = w.Max
	} else {
		v = reflect.ValueOf(data)
		k = v.Kind()

		s.id = v.String()
		s.value = data
		s.max = ^uint16(0) // Fetch the max uint16 value.
	}

	if k != reflect.Func {
		panic(IsNotFunc)
	}
	b.steps = append(b.steps, s)

	return b
}

// NewBranch
// is a constructor function that initializes heap memory for a Branch.
func NewBranch() *Branch {

	b := new(Branch)
	b.steps = make([]Step, 0)

	return b
}

// B
// is syntactic-sugar function that makes it simpler to define complex pipelines.
func B(values ...any) *Branch {

	b := NewBranch()
	for _, v := range values {
		b.Add(v)
	}

	return b
}
