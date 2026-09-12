// Package ScalingFunctions
//
// Copyright (c) 2026. Gabriel Cordovado
// All rights reserved.
//
// Source file:  pipeline_test.go

package ScalingFunctions

import (
	"fmt"
	"testing"
)

func Test_buildPipeline_EmptyPipelineIR(t *testing.T) {

	pipelineIR := PipelineIR{}
	pipeline, err := buildPipeline(&pipelineIR)
	if err != nil {
		t.Errorf("did not expect `buildPipeline` to return the error %s\n", err.Error())
		return
	}

	if len(pipeline.channels) != 0 {
		t.Error("expected the number of channels to be zero in the pipeline")
		return
	}

	if len(pipeline.functions) != 0 {
		t.Error("expected the number of functions to be zero in the pipeline")
		return
	}

	if len(pipeline.roots) != 0 {
		t.Error("expected the number of roots to be zero in the pipeline")
		return
	}

	if len(pipeline.tails) != 0 {
		t.Error("expected the number of tails to be zero in the pipeline")
		return
	}

	fmt.Println(pipeline)
}

func Test_buildPipeline_SingleHeadSingleTail(t *testing.T) {

	pipelineIR := PipelineIR{
		Identifier: "foo",
		Functions: []FunctionIR{
			FunctionIR{
				Module:     "common",
				Identifier: "extract",
				From:       "",
				To:         "et-pipe",
				StartWith:  1,
				Maximum:    1,
				Parameters: []string{},
				Returns:    []string{},
			},
			FunctionIR{
				Module:     "common",
				Identifier: "transform",
				From:       "et-pipe",
				To:         "tl-pipe",
				StartWith:  1,
				Maximum:    1,
				Parameters: []string{},
				Returns:    []string{},
			},
			FunctionIR{
				Module:     "common",
				Identifier: "load",
				From:       "tl-pipe",
				To:         "",
				StartWith:  1,
				Maximum:    1,
				Parameters: []string{},
				Returns:    []string{},
			},
		},
		Pipes: []PipeIR{
			PipeIR{
				Identifier:   "et-pipe",
				Threshold:    10,
				GrowthFactor: 2.0,
			},
			PipeIR{
				Identifier:   "tl-pipe",
				Threshold:    10,
				GrowthFactor: 2.0,
			},
		},
	}

	pipeline, err := buildPipeline(&pipelineIR)
	if err != nil {
		t.Errorf("did not expect `buildPipeline` to return the error %s\n", err.Error())
		return
	}

	if len(pipeline.channels) != 2 {
		t.Error("expected the number of channels to be 2 in the pipeline")
		return
	}

	if len(pipeline.functions) != 3 {
		t.Error("expected the number of functions to be 3 in the pipeline")
		return
	}

	if len(pipeline.roots) != 1 {
		t.Error("expected the number of roots to be 1 in the pipeline")
		return
	}

	if len(pipeline.tails) != 1 {
		t.Error("expected the number of tails to be 1 in the pipeline")
		return
	}

	fmt.Println(pipeline)
}
