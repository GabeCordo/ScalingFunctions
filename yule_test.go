package yule

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

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
	fmt.Println(d)
}

func TestBranchBuild(t *testing.T) {

	b := NewBranch().Add(add).Add(mul).Add(prt)
	b2 := NewBranch().Add(add2).Add(mul)

	metadata := Build(b, b2)

	fmt.Println(metadata)
}

func TestBuildFrom(t *testing.T) {

	d := PipelineMetadata{
		Functions: []FunctionMetadata{
			FunctionMetadata{Module: "common", Identifier: "gen", To: "0", StartWith: 1, Maximum: 1},
			FunctionMetadata{Module: "common", Identifier: "mul", From: "0", To: "1", StartWith: 1, Maximum: 1},
			FunctionMetadata{Module: "common", Identifier: "prt", From: "1", StartWith: 1, Maximum: 1},
		},
		Pipes: []PipeMetadata{
			PipeMetadata{Identifier: "0", Threshold: 1, GrowthFactor: 2.0},
			PipeMetadata{Identifier: "1", Threshold: 1, GrowthFactor: 2.0},
		},
	}

	r := NewRepository()
	r.Module("common").Map(map[string]any{
		"gen": gen,
		"mul": mul,
		"prt": prt,
	})

	Run(Build(&d, r))
}

func TestBranchRun(t *testing.T) {

	b := NewBranch().Add(gen).Add(mul).Add(prt)
	Run(Build(b))
}

func TestBranchWrapperRun(t *testing.T) {

	b := NewBranch().Add(FunctionLink{Id: "extract", Value: gen}).Add(FunctionLink{Id: "transform", Value: mul}).Add(FunctionLink{Id: "load", Value: prt})
	Run(Build(b))
}

func TestFunctionRun(t *testing.T) {

	Run(Build(gen, mul, prt))
}

func TestFunctionWrapperRun(t *testing.T) {

	Run(Build(FunctionLink{Id: "extract", Value: gen}, FunctionLink{Id: "transform", Value: mul}, FunctionLink{Id: "load", Value: prt}))
}

func stressExtract(out chan string) {

	// assumption: data is pulled from database and pushed to transform in another action
	for i := 0; i < 1000000; i++ {
		out <- "foo"
	}

	time.Sleep(20 * time.Second)

	for i := 0; i < 500000; i++ {
		out <- "bar"
	}

	close(out)
}

func stressTransform(in string) (out string, err error) {

	// "foo" and "bar" are the only valid types
	if (in != "foo") && (in != "bar") {
		return "", errors.New("'in' must be of value ('foo' or 'bar')")
	}

	// assumption: processing some unit of data takes 4ms
	time.Sleep(4 * time.Millisecond)

	return in, nil
}

func stressLoad(in string) {

	// assumption: uploading to database takes 3ms
	time.Sleep(3 * time.Millisecond)
}

//func TestFunctionStress(t *testing.T) {
//	Pipeline := Build(FunctionLink{Value: stressExtract}, FunctionLink{Value: stressTransform}, FunctionLink{Value: stressLoad})
//	Pipeline.Run()
//}
