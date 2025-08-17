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
}
