// Package ScalingFunctions
//
// Copyright (c) 2024-2026. Gabriel Cordovado
// All rights reserved.
//
// Source file: scalingfunctions_test.go
package ScalingFunctions

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/GabeCordo/ScalingFunctions/internal/builders"
	"github.com/GabeCordo/ScalingFunctions/internal/flags"
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

// setupLogOutput
// directs output to the `buf` parameter for validation.
func setupLogOutput() (buf *bytes.Buffer) {

	buf = new(bytes.Buffer)
	log.SetOutput(buf)
	log.SetFlags(0) // do not show timestamp

	return buf
}

// validateGenMulPrt
// verifies a text is present inside the `buf` parameter.
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
//						Base Functionality Tests
////////////////////////////////////////////////////////////////////////
//
//	Base functionality tests ensure the framework operates as expected
//  during sunny day scenarios.
//
//	These tests replicate how the framework is used by the average user
//  and strays away from validating edge-case scenarios. The reader may
//  use these tests as reference for using the framework.
//
////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////
//
// Test  Branch::Add( ... )
//	 ∟ Test_BranchAdd_FunctionArgument
//	 ∟ Test_BranchAdd_FArgument
//
////////////////////////////////////////////////////////////////////////

// Test_BranchAdd_FunctionArgument
// tests that the `Build` function succeeds in creating a pipeline
// when passed one-to-many `*Branch` arguments.
func Test_BranchAdd_FunctionArgument(t *testing.T) {

	b := NewBranch().Add(add).Add(mul).Add(prt)
	b2 := NewBranch().Add(add2).Add(mul)

	interactable := Build(b, b2)

	if len(interactable.pipeline.functions) != 4 {
		t.Error("expected the pipeline to have 4 functions.")
	}

	if len(interactable.pipeline.channels) != 2 {
		t.Error("expected the pipeline to have 2 channels.")
	}
}

// Test_BranchAdd_FArgument
// tests that the `Branch::Add` function succeeds when being passed a
// `F` structure. The `F` structure includes metadata and the function
// reference.
func Test_BranchAdd_FArgument(t *testing.T) {

	var buf *bytes.Buffer
	if flags.GO_RACE_CHECKER_DISABLED {
		buf = setupLogOutput()
	}

	b := NewBranch().Add(F{Id: "extract", Value: gen}).Add(F{Id: "transform", Value: mul}).Add(F{Id: "load", Value: prt})
	err := Build(b).Run()
	if err != nil {
		t.Error(err)
	}

	if flags.GO_RACE_CHECKER_DISABLED {
		ee := validateGenMulPrt(buf)
		for _, e := range ee {
			t.Error(e)
		}
	}
}

////////////////////////////////////////////////////////////////////////
//
// Test  Module::Map( ... )
//	 ∟ Test_ModuleMap
//
////////////////////////////////////////////////////////////////////////

// Test_ModuleMap
// tests that the `Build` function succeeds in creating a pipeline
// when passed a `PipelineIR` and `*Repository` argument.
//
// The `*Repository` is creating using the `Repository::Map` function.
func Test_ModuleMap(t *testing.T) {

	var buf *bytes.Buffer
	if flags.GO_RACE_CHECKER_DISABLED {
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

	if flags.GO_RACE_CHECKER_DISABLED {
		ee := validateGenMulPrt(buf)
		for _, e := range ee {
			t.Error(e)
		}
	}
}

////////////////////////////////////////////////////////////////////////
//
// Test  Module::LinkFunction( ... )
//	 ∟ Test_ModuleLinkFunction
//
////////////////////////////////////////////////////////////////////////

// Test_ModuleLinkFunction
// tests that the `Build` function succeeds in creating a pipeline
// when passed a `PipelineIR` and `*Repository` argument.
//
// The `*Repository` is created using the `Repository::Module` and
// `Module::LinkFunction` functions.
func Test_ModuleLinkFunction(t *testing.T) {

	var buf *bytes.Buffer
	if flags.GO_RACE_CHECKER_DISABLED {
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

	if flags.GO_RACE_CHECKER_DISABLED {
		ee := validateGenMulPrt(buf)
		for _, e := range ee {
			t.Error(e)
		}
	}
}

////////////////////////////////////////////////////////////////////////
//
// Test  Pipeline::Run( ... )
//	 ∟ Test_PipelineRun_BranchArgument
//	 ∟ Test_PipelineRun_FuncArgument
//   ∟ Test_PipelineRun_FArgument
//
////////////////////////////////////////////////////////////////////////

// Test_PipelineRun_BranchArgument
// tests that the `Pipeline::Run` function succeeds when the `Pipeline`
// was built using branch arguments.
func Test_PipelineRun_BranchArgument(t *testing.T) {

	var buf *bytes.Buffer
	if flags.GO_RACE_CHECKER_DISABLED {
		buf = setupLogOutput()
	}

	b := NewBranch().Add(gen).Add(mul).Add(prt)
	err := Build(b).Run()
	if err != nil {
		t.Error(err)
	}

	if flags.GO_RACE_CHECKER_DISABLED {
		ee := validateGenMulPrt(buf)
		for _, e := range ee {
			t.Error(e)
		}
	}
}

// Test_PipelineRun_FuncArgument
// tests that the `Pipeline::Run` function succeeds when the `Pipeline`
// was built using function arguments.
func Test_PipelineRun_FuncArgument(t *testing.T) {

	var buf *bytes.Buffer
	if flags.GO_RACE_CHECKER_DISABLED {
		buf = setupLogOutput()
	}

	err := Build(gen, mul, prt).Run()
	if err != nil {
		t.Error(err)
	}

	if flags.GO_RACE_CHECKER_DISABLED {
		ee := validateGenMulPrt(buf)
		for _, e := range ee {
			t.Error(e)
		}
	}
}

// Test_PipelineRun_FArgument
// // tests that the `Pipeline::Run` function succeeds when the `Pipeline`
// was built using `F` struct arguments.
func Test_PipelineRun_FArgument(t *testing.T) {

	var buf *bytes.Buffer
	if flags.GO_RACE_CHECKER_DISABLED {
		buf = setupLogOutput()
	}

	err := Build(F{Id: "extract", Value: gen}, F{Id: "transform", Value: mul}, F{Id: "load", Value: prt}).Run()
	if err != nil {
		t.Error(err)
	}

	if flags.GO_RACE_CHECKER_DISABLED {
		ee := validateGenMulPrt(buf)
		for _, e := range ee {
			t.Error(e)
		}
	}
}

////////////////////////////////////////////////////////////////////////
//
// Test  Build( ... )
//	 ∟ Test_Build_FunctionFollowedByInvalidArgument
//	 ∟ Test_Build_FunctionWrapperFollowedByInvalidArgument
//	 ∟ Test_Build_BranchFollowedByInvalidArgument
//	 ∟ Test_Build_RepositoryFollowedByInvalidArgument
//	 ∟ Test_Build_ConfigFollowedByInvalidArgument
// 	 ∟ Test_Build_InvalidArgument
//
////////////////////////////////////////////////////////////////////////

// Test_Build_FunctionFollowedByInvalidArgument
// tests that the `Pipeline::Build` function fails when receiving a
// an unexpected argument after a valid function.
func Test_Build_FunctionFollowedByInvalidArgument(t *testing.T) {

	defer func() {
		if r := recover(); r != nil {
			t.Log("succesfully paniced when Build() received an invalid value")
		}
	}()

	var invalidType *int = nil
	Build(gen, invalidType)
	t.Error("expected panic() to be called when an invalid argument is provided")
}

// Test_Build_FunctionWrapperFollowedByInvalidArgument
// tests that the `Pipeline::Build` function fails when receiving a
// an unexpected argument after a valid function wrapper.
func Test_Build_FunctionWrapperFollowedByInvalidArgument(t *testing.T) {

	defer func() {
		if r := recover(); r != nil {
			t.Log("succesfully paniced when Build() received an invalid value")
		}
	}()

	var invalidType *int = nil
	Build(F{Id: "extract", Value: gen}, invalidType)
	t.Error("expected panic() to be called when an invalid argument is provided")
}

// Test_Build_BranchFollowedByInvalidArgument
// tests that the `Pipeline::Build` function fails when receiving a
// an unexpected argument after a valid branch.
func Test_Build_BranchFollowedByInvalidArgument(t *testing.T) {

	defer func() {
		if r := recover(); r != nil {
			t.Log("succesfully paniced when Build() received an invalid value")
		}
	}()

	branch := NewBranch()

	var invalidType *int = nil
	Build(branch, invalidType)
	t.Error("expected panic() to be called when an invalid argument is provided")
}

// Test_Build_RepositoryFollowedByInvalidArgument
// tests that the `Pipeline::Build` function fails when receiving a
// an unexpected argument after a valid repository.
func Test_Build_RepositoryFollowedByInvalidArgument(t *testing.T) {

	defer func() {
		if r := recover(); r != nil {
			t.Log("succesfully paniced when Build() received an invalid value")
		}
	}()

	repository := NewRepository()

	var invalidType *int = nil
	Build(repository, invalidType)
	t.Error("expected panic() to be called when an invalid argument is provided")
}

// Test_Build_ConfigFollowedByInvalidArgument
// tests that the `Pipeline::Build` function fails when receiving a
// an unexpected argument after a valid config.
func Test_Build_ConfigFollowedByInvalidArgument(t *testing.T) {

	defer func() {
		if r := recover(); r != nil {
			t.Log("succesfully paniced when Build() received an invalid value")
		}
	}()

	pipelineIR := &PipelineIR{}

	var invalidType *int = nil
	Build(&pipelineIR, invalidType)
	t.Error("expected panic() to be called when an invalid argument is provided")
}

// Test_Build_InvalidArgument
// tests that the `Pipeline::Build` function fails when receiving a
// an invalid argument.
func Test_Build_InvalidArgument(t *testing.T) {

	defer func() {
		if r := recover(); r != nil {
			t.Log("succesfully paniced when Build() received an invalid value")
		}
	}()

	var invalidType *int = nil
	Build(invalidType)
	t.Error("expected panic() to be called when an invalid argument is provided")
}

////////////////////////////////////////////////////////////////////////
//							Stress Tests
////////////////////////////////////////////////////////////////////////
//
//	Stress test scenarios push the framework under heavy data loads
//	and validate that growth and shrinkage of the provisioned functions
// 	aligns with the configured parameters.
//
////////////////////////////////////////////////////////////////////////

// TestStressScenario
// tests how the pipeline behaves when a series of functions have different
// processing rates.
//
// the framework must provision new goroutines when data piles up in the queue.
func TestStressScenario(t *testing.T) {

	var numberOfPackets uint64 = 1000

	gFunc := builders.CreateGeneratorFunction(1*time.Millisecond, int(numberOfPackets))
	p1Func := builders.CreateProcessorFunction(2 * time.Nanosecond)
	p2Func := builders.CreateProcessorFunction(5 * time.Nanosecond)
	lFunc := builders.CreateLoadFunction(1 * time.Millisecond)

	i := Build(gFunc, p1Func, p2Func, lFunc)

	c := make(chan int)

	go func(c chan int) {
		i.Run()
		c <- 0
	}(c)

	ss := make([]Statistics, 0)

	pipelineDone := false
	for {
		select {
		case <-c:
			{
				pipelineDone = true
			}
		default:
			{
				s := *i.GetStatistics()
				ss = append(ss, s)
				time.Sleep(100 * time.Millisecond)
			}
		}

		if pipelineDone {
			s := *i.GetStatistics()
			ss = append(ss, s)
			break
		}
	}

	if len(ss) < 1 {
		t.Error("expected at least one statistic captured")
		return
	}

	t.Logf("Captured %d statistics\n", len(ss))

	if ss[len(ss)-1].Pipes[0].Pulled != numberOfPackets {
		t.Errorf("expected the number of packets in the final statistic capture to equal %d, got %d\n", numberOfPackets, ss[len(ss)-1].Pipes[0].Pulled)
	}
}
