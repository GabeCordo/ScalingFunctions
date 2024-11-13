package yule

import (
	"errors"
	"reflect"
)

var IsNotFunc = errors.New("passed value must be of type function")

type M struct {
	identifier string
}

type Step struct {
	id    string
	value any
}

// Branch
// is a series of functions chained one after another.
type Branch struct {
	steps []Step
}

func NewBranch() *Branch {

	b := new(Branch)
	b.steps = make([]Step, 0)

	return b
}

func (b *Branch) Add(value any, m ...M) *Branch {

	v := reflect.ValueOf(value)
	k := v.Kind()

	if k != reflect.Func {
		panic(IsNotFunc)
	}

	s := Step{value: value}

	b.steps = append(b.steps, s)

	return b
}
