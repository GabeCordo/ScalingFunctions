// Package ScalingFunctions
//
// Copyright (c) 2024-2025. Gabriel Cordovado
// All rights reserved.
//
// Source file:  branch_test.go
package ScalingFunctions

import (
	"testing"
	"time"

	"github.com/GabeCordo/ScalingFunctions/internal/builders"
)

////////////////////////////////////////////////////////////////////////
//
// Test  F::GetIR( ... )
//	 ∟ Test_FGetIR
//
////////////////////////////////////////////////////////////////////////

func Test_FGetIR(t *testing.T) {

	fValue := builders.CreateProcessorFunction(1 * time.Millisecond)

	var fIdentifier string = "foo"
	var fMax uint16 = 1

	fMetadata := F{Id: fIdentifier, Max: fMax, Value: fValue}
	fIR := fMetadata.GetIR()

	if fIR.Identifier != fIdentifier {
		t.Errorf("expected FunctionIR::Identifier to be %s but received %s\n",
			fIdentifier, fIR.Identifier)
		return
	}

	if fIR.Maximum != fMax {
		t.Errorf("expected FunctionIR::Maximum to be %d but received %d\n",
			fMax, fIR.Maximum)
		return
	}
}

////////////////////////////////////////////////////////////////////////
//
// Test  Branch::Add( ... )
//	 ∟ Test_BranchAdd
//	 ∟ Test_BranchAdd_NotAFunction
//
////////////////////////////////////////////////////////////////////////

func Test_BranchAdd(t *testing.T) {

	fValue := builders.CreateProcessorFunction(1 * time.Millisecond)

	b := B()
	if b == nil {
		t.Error("expected a valid Branch* pointer")
		return
	}

	if len(b.steps) > 0 {
		t.Error("expected the number of steps to be zero")
		return
	}

	b.Add(fValue)

	if len(b.steps) != 1 {
		t.Error("expected the number of steps to be one")
		return
	}
}

func Test_BranchAdd_NotAFunction(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Log("succesfully paniced when Branch::Add() received an invalid value")
		}
	}()

	var invalidValue *int = nil

	b := B()
	if b == nil {
		t.Error("expected a valid Branch* pointer")
		return
	}

	b.Add(invalidValue)
	t.Error("expected Branch::Add() to panic when receiving a non-function type")
}

////////////////////////////////////////////////////////////////////////
//
// Test  B( ... )
//	 ∟ Test_B_MultipleFunctions
//
////////////////////////////////////////////////////////////////////////

func Test_B_MultipleFunctions(t *testing.T) {

	f1Value := builders.CreateProcessorFunction(1 * time.Millisecond)
	f2Value := builders.CreateProcessorFunction(1 * time.Millisecond)
	f3Value := builders.CreateProcessorFunction(1 * time.Millisecond)

	b := B(f1Value, f2Value, f3Value)
	if b == nil {
		t.Error("expected a valid Branch* pointer")
		return
	}

	if len(b.steps) != 3 {
		t.Error("expected the number of steps to be three")
		return
	}
}
