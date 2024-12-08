package yule

import (
	"fmt"
	"testing"
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

	d := Deployment{
		Functions: []Function{
			Function{Module: "common", Identifier: "gen", To: "0"},
			Function{Module: "common", Identifier: "mul", From: "0", To: "1"},
			Function{Module: "common", Identifier: "prt", From: "1"},
		},
		Pipes: []Pipe{
			Pipe{Identifier: "0", Threshold: 1, GrowthFactor: 2.0},
			Pipe{Identifier: "1", Threshold: 1, GrowthFactor: 2.0},
		},
	}

	r := NewRepository()
	r.Module("common").Map(map[string]any{
		"gen": gen,
		"mul": mul,
		"prt": prt,
	})

	Build(&d, r).Run()
}

func TestBranchRun(t *testing.T) {

	b := NewBranch().Add(gen).Add(mul).Add(prt)
	Build(b).Run()
}

func TestBranchWrapperRun(t *testing.T) {

	b := NewBranch().Add(F{"extract", gen}).Add(F{"transform", mul}).Add(F{"load", prt})
	Build(b).Run()
}

func TestFunctionRun(t *testing.T) {

	Build(gen, mul, prt).Run()
}

func TestFunctionWrapperRun(t *testing.T) {

	Build(F{"extract", gen}, F{"transform", mul}, F{"load", prt}).Run()
}
