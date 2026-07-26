// Package ScalingFunctions
//
// Copyright (c) 2024-2026. Gabriel Cordovado
// All rights reserved.
//
// Source file:  plover_test.go
package ScalingFunctions

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"testing"

	"github.com/GabeCordo/ScalingFunctions/internal"
)

////////////////////////////////////////////////////////////////////////
//					Pipeline Functions to Run
////////////////////////////////////////////////////////////////////////

func gen(out chan int) {

	defer close(out)

	for i := 0; i < 10; i++ {
		out <- i
	}
}

func add(a, b int) (c int) {
	c = a + b
	return c
}

func add2(a int) (c int) {
	c = a + 2
	return c
}

func mul(c int) (d int) {
	d = c * 2
	return d
}

func prt(d int) {
	log.Println(d)
}

////////////////////////////////////////////////////////////////////////
//						Test Helper Functions
////////////////////////////////////////////////////////////////////////

func setupLogOutput() (buf *bytes.Buffer) {

	buf = new(bytes.Buffer)
	log.SetOutput(buf)
	log.SetFlags(0) // do not show timestamp

	return buf
}

func validateGenMulPrt(buf *bytes.Buffer) (err []error) {

	err = make([]error, 0)

	stdOut := buf.String()
	linesInStdOut := strings.Split(stdOut, "\n")
	linesInStdOut = linesInStdOut[:len(linesInStdOut)-1] // there will be one extra index we want to remove.

	for idx, line := range linesInStdOut {
		expectedValue := idx * 2                    // output of 'gen' + 'mul'
		expectedLine := strconv.Itoa(expectedValue) // output of 'prt'

		if line != expectedLine {
			output := fmt.Sprintf("at index %d, expected: %d, received: %s", idx, idx*2, line)
			err = append(err, errors.New(output))
		}
	}

	return err
}

////////////////////////////////////////////////////////////////////////
//								Tests
////////////////////////////////////////////////////////////////////////

func TestBranchBuild(t *testing.T) {

	b := NewBranch().Add(add).Add(mul).Add(prt)
	b2 := NewBranch().Add(add2).Add(mul)

	pipeline := Build(b, b2)

	if len(pipeline.functions) != 4 {
		t.Error("expected the pipeline to have 4 functions.")
	}

	if len(pipeline.channels) != 2 {
		t.Error("expected the pipeline to have 2 channels.")
	}
}

func TestBuildFrom_Map(t *testing.T) {

	var buf *bytes.Buffer
	if internal.GO_RACE_CHECKER_DISABLED {
		buf = setupLogOutput()
	}

	d := PipelineIR{
		Functions: []FunctionIR{
			FunctionIR{Module: "common", Identifier: "gen", To: "0", StartWith: 1, Maximum: 1},
			FunctionIR{Module: "common", Identifier: "mul", From: "0", To: "1", StartWith: 1, Maximum: 1},
			FunctionIR{Module: "common", Identifier: "prt", From: "1", StartWith: 1, Maximum: 1},
		},
		Pipes: []PipeIR{
			PipeIR{Identifier: "0", Threshold: 1, GrowthFactor: 2.0},
			PipeIR{Identifier: "1", Threshold: 1, GrowthFactor: 2.0},
		},
	}

	r := NewRepository()
	err := r.Module("common").Map(map[string]any{
		"gen": gen,
		"mul": mul,
		"prt": prt,
	})
	if err != nil {
		t.Error(err)
	}

	err = Build(&d, r).Run()
	if err != nil {
		t.Error(err)
	}

	if internal.GO_RACE_CHECKER_DISABLED {
		ee := validateGenMulPrt(buf)
		for _, e := range ee {
			t.Error(e)
		}
	}
}

func TestBuildFrom_LinkFunction(t *testing.T) {

	var buf *bytes.Buffer
	if internal.GO_RACE_CHECKER_DISABLED {
		buf = setupLogOutput()
	}

	d := PipelineIR{
		Functions: []FunctionIR{
			FunctionIR{Module: "common", Identifier: "gen", To: "0", StartWith: 1, Maximum: 1},
			FunctionIR{Module: "common", Identifier: "mul", From: "0", To: "1", StartWith: 1, Maximum: 1},
			FunctionIR{Module: "common", Identifier: "prt", From: "1", StartWith: 1, Maximum: 1},
		},
		Pipes: []PipeIR{
			PipeIR{Identifier: "0", Threshold: 1, GrowthFactor: 2.0},
			PipeIR{Identifier: "1", Threshold: 1, GrowthFactor: 2.0},
		},
	}

	r := NewRepository()
	m := r.Module("common")

	err := m.LinkFunction("gen", gen)
	if err != nil {
		t.Error(err)
	}
	err = m.LinkFunction("mul", mul)
	if err != nil {
		t.Error(err)
	}
	err = m.LinkFunction("prt", prt)
	if err != nil {
		t.Error(err)
	}

	err = Build(&d, r).Run()
	if err != nil {
		t.Error(err)
	}

	if internal.GO_RACE_CHECKER_DISABLED {
		ee := validateGenMulPrt(buf)
		for _, e := range ee {
			t.Error(e)
		}
	}
}

func TestBranchRun(t *testing.T) {

	var buf *bytes.Buffer
	if internal.GO_RACE_CHECKER_DISABLED {
		buf = setupLogOutput()
	}

	b := NewBranch().Add(gen).Add(mul).Add(prt)
	err := Build(b).Run()
	if err != nil {
		t.Error(err)
	}

	if internal.GO_RACE_CHECKER_DISABLED {
		ee := validateGenMulPrt(buf)
		for _, e := range ee {
			t.Error(e)
		}
	}
}

func TestBranchWrapperRun(t *testing.T) {

	var buf *bytes.Buffer
	if internal.GO_RACE_CHECKER_DISABLED {
		buf = setupLogOutput()
	}

	b := NewBranch().Add(F{Id: "extract", Value: gen}).Add(F{Id: "transform", Value: mul}).Add(F{Id: "load", Value: prt})
	err := Build(b).Run()
	if err != nil {
		t.Error(err)
	}

	if internal.GO_RACE_CHECKER_DISABLED {
		ee := validateGenMulPrt(buf)
		for _, e := range ee {
			t.Error(e)
		}
	}
}

func TestFunctionRun(t *testing.T) {

	var buf *bytes.Buffer
	if internal.GO_RACE_CHECKER_DISABLED {
		buf = setupLogOutput()
	}

	err := Build(gen, mul, prt).Run()
	if err != nil {
		t.Error(err)
	}

	if internal.GO_RACE_CHECKER_DISABLED {
		ee := validateGenMulPrt(buf)
		for _, e := range ee {
			t.Error(e)
		}
	}
}

func TestFunctionWrapperRun(t *testing.T) {

	var buf *bytes.Buffer
	if internal.GO_RACE_CHECKER_DISABLED {
		buf = setupLogOutput()
	}

	err := Build(F{Id: "extract", Value: gen}, F{Id: "transform", Value: mul}, F{Id: "load", Value: prt}).Run()
	if err != nil {
		t.Error(err)
	}

	if internal.GO_RACE_CHECKER_DISABLED {
		ee := validateGenMulPrt(buf)
		for _, e := range ee {
			t.Error(e)
		}
	}
}
