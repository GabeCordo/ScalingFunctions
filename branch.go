package yule

import (
	"errors"
	"log"
	"reflect"
)

var IsNotFunc = errors.New("passed value must be of type function")

// Step
// is a wrapper to associate a function to a string identifier.
type Step struct {
	id    string
	value any
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
func (b *Branch) Add(value any, m ...M) *Branch {

	v := reflect.ValueOf(value)
	k := v.Kind()

	if k != reflect.Func {
		panic(IsNotFunc)
	}

	s := Step{value: value}

	if len(m) > 0 {
		s.id = m[0].id
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

	for idx, value := range values {
		if reflect.TypeOf(value).Kind() != reflect.Func {
			log.Panicf("value passed to B at index %d is not a function\n", idx)
		}
	}

	b := new(Branch)

	b.steps = make([]Step, len(values))
	for idx, value := range values {
		b.steps[idx] = Step{value: value}
	}

	return b
}
