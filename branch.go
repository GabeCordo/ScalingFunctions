package yule

import (
	"errors"
	"reflect"
)

var IsNotFunc = errors.New("passed value must be of type function")

// Step
// is a wrapper to associate a function to a string identifier.
type Step struct {
	id    string
	value any
	max   int
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

// Add
// allows the developer to define new function steps using the builder pattern.
func (b *Branch) Add(data any) *Branch {

	var v reflect.Value
	var k reflect.Kind

	var s Step

	if w, isWrapperType := data.(FunctionLink); isWrapperType {
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
		s.max = -1
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
