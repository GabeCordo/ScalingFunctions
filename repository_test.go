// Package ScalingFunctions
//
// Copyright (c) 2024-2026. Gabriel Cordovado
// All rights reserved.
//
// Source file:  repository_test.go
package ScalingFunctions

import (
	"fmt"
	"testing"
)

func addTwo(a, b int) int {
	return a + b
}

func print(a int) {
	fmt.Println(a)
}

func TestModule_GetIR(t *testing.T) {

	repository := NewRepository()
	mod := repository.Module("common")
	mod.LinkFunction("add", addTwo)
	mod.LinkFunction("print", print)

	ir := mod.GetIR()

	if ir.Identifier != "common" {
		t.Error("expected the moduleIR to have an identifier of 'common'")
	}

	for idx, function := range ir.Functions {

		if (idx == 0) && function.Identifier != "add" {
			t.Error("expected the moduleIR to have an identifier of 'add'")
		} else if (idx == 1) && function.Identifier != "print" {
			t.Error("expected the moduleIR to have an identifier of 'print'")
		}

	}
}
