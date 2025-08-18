package plover

import (
	"testing"
	"time"
)

////////////////////////////////////////////////////////////////////////
//					Pipeline Functions to Run
////////////////////////////////////////////////////////////////////////

func simulateExtract(out chan int) {
	defer close(out)
	for i := 0; i < 1000000; i++ {
		time.Sleep(10 * time.Millisecond)
		out <- i
	}
}

func simulateTransform(a int) (b int) {
	b = a
	time.Sleep(2 * time.Millisecond)
	return b
}

func simulateLoad(a int) {
	time.Sleep(10 * time.Millisecond)
}

////////////////////////////////////////////////////////////////////////
//								Tests
////////////////////////////////////////////////////////////////////////

func TestPipeline_Close(t *testing.T) {

	p := Build(F{
		Id: "extract", Value: simulateExtract},
		F{Id: "transform", Value: simulateTransform},
		F{Id: "load", Value: simulateLoad},
	)
	i := p.Interactable()

	go func(i Interactable) {
		err := i.Run()
		if err != nil {
			t.Error(err)
		}
	}(i)

	i.Stop()
	time.Sleep(1 * time.Second)
}

func TestPipeline_Close_BuiltFrom(t *testing.T) {

	d := PipelineIR{
		Functions: []FunctionIR{
			FunctionIR{Module: "common", Identifier: "extract", To: "0", StartWith: 1},
			FunctionIR{Module: "common", Identifier: "transform", From: "0", To: "1", StartWith: 1},
			FunctionIR{Module: "common", Identifier: "load", From: "1", StartWith: 1},
		},
		Pipes: []PipeIR{
			PipeIR{Identifier: "0", Threshold: 1, GrowthFactor: 2.0},
			PipeIR{Identifier: "1", Threshold: 1, GrowthFactor: 2.0},
		},
	}

	r := NewRepository()
	err := r.Module("common").Map(map[string]any{
		"extract":   simulateExtract,
		"transform": simulateTransform,
		"load":      simulateLoad,
	})
	if err != nil {
		t.Error(err)
	}

	p := Build(&d, r)
	i := p.Interactable()

	go func(i Interactable) {
		err = i.Run()
		if err != nil {
			t.Error(err)
		}
	}(i)

	i.Stop()
	time.Sleep(1 * time.Second)
}
