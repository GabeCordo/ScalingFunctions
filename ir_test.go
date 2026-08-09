// Package ScalingFunctions
//
// Copyright (c) 2024-2026. Gabriel Cordovado
// All rights reserved.
//
// Source file:  ir_test.go
package ScalingFunctions

import (
	"errors"
	"testing"
	"time"

	"github.com/GabeCordo/ScalingFunctions/internal/builders"
)

func generateValidModuleIR() *ModuleIR {
	return &ModuleIR{
		Identifier: "common",
		Version:    "v0.1",
		Functions:  make([]FunctionIR, 0),
	}
}

////////////////////////////////////////////////////////////////////////
//
// Test  buildPipelineIR( ... )
//	 ∟ Test_buildPipelineIR_NoBranches
//	 ∟ Test_buildPipelineIR_ValidBranche
//
////////////////////////////////////////////////////////////////////////

func Test_buildPipelineIR_NoBranches(t *testing.T) {

	pipelineIR, err := buildPipelineIR()
	if err != nil {
		t.Error(err)
	}

	if pipelineIR.Pipes == nil {
		t.Error("expected pipelineIR::Pipes to be non nil")
	}

	if len(pipelineIR.Pipes) != 0 {
		t.Error("expected size of pipelineIR::Pipes to be zero")
	}

	if pipelineIR.Functions == nil {
		t.Error("expected pipelineIR::Functions to be non nil")
	}

	if len(pipelineIR.Functions) != 0 {
		t.Error("expected size of pipelineIR::Functions to be zero")
	}
}

func Test_buildPipelineIR_ValidBranche(t *testing.T) {

	startFunc := builders.CreateGeneratorFunction(1*time.Millisecond, 10)
	branch := NewBranch().Add(startFunc)

	pipelineIR, err := buildPipelineIR(branch)
	if err != nil {
		t.Error(err)
	}

	if pipelineIR.Pipes == nil {
		t.Error("expected pipelineIR::Pipes to be non nil")
	}

	if len(pipelineIR.Pipes) != 0 {
		t.Error("expected size of pipelineIR::Pipes to be 0")
	}

	if pipelineIR.Functions == nil {
		t.Error("expected pipelineIR::Functions to be non nil")
	}

	if len(pipelineIR.Functions) != 1 {
		t.Error("expected size of pipelineIR::Functions to be 1")
	}
}

////////////////////////////////////////////////////////////////////////
//
// Test  VerifyIR( ... )
//	 ∟ Test_VerifyIR_Valid
//	 ∟ Test_VerifyIR_MissingModuleIdentifier
//	 ∟ Test_VerifyIR_MissingModuleVersion
//	 ∟ Test_VerifyIR_ValidModuleVersionVFormat
//	 ∟ Test_VerifyIR_ValidModuleVersionFloatFormat
//	 ∟ Test_VerifyIR_FunctionIdentifierValid
//	 ∟ Test_VerifyIR_FunctionIdentifierConflict
//	 ∟ Test_VerifyIR_FunctionIR_NotSupport
//	 ∟ Test_VerifyIR_PipelineIR_NotSupport
//	 ∟ Test_VerifyIR_PipeIR_NotSupport
//	 ∟ Test_VerifyIR_UnknownType_NotSupport
//
////////////////////////////////////////////////////////////////////////

func Test_VerifyIR_Valid(t *testing.T) {

	ir := generateValidModuleIR()

	ee := VerifyIR(ir)
	if len(ee) > 0 {
		t.Errorf("expected IR verification to succeed")
	}
}

func Test_VerifyIR_MissingModuleIdentifier(t *testing.T) {

	ir := generateValidModuleIR()
	ir.Identifier = ""

	ee := VerifyIR(ir)
	if len(ee) != 1 {
		t.Errorf("expected at least on IR verification to fail")
		return
	}

	if !errors.Is(ee[0], EmptyIdentifier) {
		t.Errorf("expected IR verification to fail on 'EmptyIdentifier'")
	}
}

func Test_VerifyIR_MissingModuleVersion(t *testing.T) {

	ir := generateValidModuleIR()
	ir.Version = ""

	ee := VerifyIR(ir)
	if len(ee) != 1 {
		t.Errorf("expected at least on IR verification to fail")
		return
	}

	if !errors.Is(ee[0], EmptyVersion) {
		t.Errorf("expected IR verification to fail on 'EmptyVersion'")
	}
}

func Test_VerifyIR_ValidModuleVersionVFormat(t *testing.T) {

	ir := generateValidModuleIR()
	ir.Version = "v0.1"

	ee := VerifyIR(ir)
	if len(ee) > 0 {
		t.Errorf("expected IR verification to pass")
		return
	}
}

func Test_VerifyIR_ValidModuleVersionFloatFormat(t *testing.T) {

	ir := generateValidModuleIR()
	ir.Version = "0.1"

	ee := VerifyIR(ir)
	if len(ee) > 0 {
		t.Errorf("expected IR verification to pass")
		return
	}
}

func Test_VerifyIR_FunctionIdentifierValid(t *testing.T) {
	ir := generateValidModuleIR()

	function1 := FunctionIR{
		Identifier: "add",
	}
	ir.Functions = append(ir.Functions, function1)

	function2 := FunctionIR{
		Identifier: "sub",
	}
	ir.Functions = append(ir.Functions, function2)

	ee := VerifyIR(ir)
	if len(ee) > 0 {
		t.Error("expected IR verification to pass")
	}
}

func Test_VerifyIR_FunctionIdentifierConflict(t *testing.T) {
	ir := generateValidModuleIR()

	function1 := FunctionIR{
		Identifier: "add",
	}
	ir.Functions = append(ir.Functions, function1)

	function2 := FunctionIR{
		Identifier: "add",
	}
	ir.Functions = append(ir.Functions, function2)

	ee := VerifyIR(ir)
	if len(ee) != 1 {
		t.Error("expected one IR verification to fail")
		return
	}

	if !errors.Is(ee[0], NamingConflict) {
		t.Errorf("expected IR verification to fail on 'NamingConflict'")
	}
}

func Test_VerifyIR_FunctionIR_NotSupport(t *testing.T) {
	fIR := FunctionIR{}

	ee := VerifyIR(&fIR)

	if len(ee) < 1 {
		t.Error("expected at least one error returned when verifying FunctionIR")
		return
	}

	if !errors.Is(ee[0], UnsupportedIRVerifyType) {
		t.Error("expected UnsupportedIRVerifyType as the failure")
		return
	}
}

func Test_VerifyIR_PipelineIR_NotSupport(t *testing.T) {
	pIR := PipelineIR{}

	ee := VerifyIR(&pIR)

	if len(ee) < 1 {
		t.Error("expected at least one error returned when verifying PipelineIR")
		return
	}

	if !errors.Is(ee[0], UnsupportedIRVerifyType) {
		t.Error("expected UnsupportedIRVerifyType as the failure")
		return
	}
}

func Test_VerifyIR_PipeIR_NotSupport(t *testing.T) {
	pIR := PipeIR{}

	ee := VerifyIR(&pIR)

	if len(ee) < 1 {
		t.Error("expected at least one error returned when verifying PipeIR")
		return
	}

	if !errors.Is(ee[0], UnsupportedIRVerifyType) {
		t.Error("expected UnsupportedIRVerifyType as the failure")
		return
	}
}

func Test_VerifyIR_UnknownType_NotSupport(t *testing.T) {
	var p *int = nil

	ee := VerifyIR(&p)

	if len(ee) < 1 {
		t.Error("expected at least one error returned when verifying invalid type")
		return
	}

	if !errors.Is(ee[0], UnsupportedIRVerifyType) {
		t.Error("expected UnsupportedIRVerifyType as the failure")
		return
	}
}

////////////////////////////////////////////////////////////////////////
//
// Test  CleanupIR( ... )
//	 ∟ Test_CleanupIR_ModuleIR_VersionEmpty
//	 ∟ Test_CleanupIR_ModuleIR_MissingMinMaxFunctionMetadata
//	 ∟ Test_CleanupIR_PipelineIR_MissingMinMaxFunctionMetadata
//
////////////////////////////////////////////////////////////////////////

// Test_CleanupIR_ModuleIR_VersionEmpty
// tests the empty version is replaced with a valid version.
func Test_CleanupIR_ModuleIR_VersionEmpty(t *testing.T) {

	m := ModuleIR{Version: ""}

	err := CleanupIR(&m)
	if err != nil {
		t.Error(err)
		return
	}

	if m.Version == "" {
		t.Error("expected the version to be transformed to a non-empty version")
	}
}

// Test_CleanupIR_ModuleIR_MissingMinMaxFunctionMetadata
// test validates that missing `Maximum` and `StartWith` fields in a `FunctionIR`
// is reset to a static value of `1`.
func Test_CleanupIR_ModuleIR_MissingMinMaxFunctionMetadata(t *testing.T) {

	f := FunctionIR{Maximum: 0, StartWith: 0}
	ff := make([]FunctionIR, 1)
	ff[0] = f
	m := ModuleIR{Version: "v0.1.0", Functions: ff}

	err := CleanupIR(&m)
	if err != nil {
		t.Error(err)
		return
	}

	if m.Functions[0].StartWith != 1 {
		t.Error("expected the StartWith to be cleaned up to `1`")
	}

	if m.Functions[0].Maximum != 1 {
		t.Error("expected the Maximum to be cleaned up to `1`")
	}
}

// Test_CleanupIR_PipelineIR_MissingMinMaxFunctionMetadata
// test validates that missing `Maximum` and `StartWith` fields in a `FunctionIR`
// is reset to a static value of `1`.
func Test_CleanupIR_PipelineIR_MissingMinMaxFunctionMetadata(t *testing.T) {

	f := FunctionIR{Maximum: 0, StartWith: 0}
	ff := make([]FunctionIR, 1)
	ff[0] = f
	p := PipelineIR{Functions: ff}

	err := CleanupIR(&p)
	if err != nil {
		t.Error(err)
		return
	}

	if p.Functions[0].StartWith != 1 {
		t.Error("expected the StartWith to be cleaned up to `1`")
	}

	if p.Functions[0].Maximum != 1 {
		t.Error("expected the Maximum to be cleaned up to `1`")
	}
}
