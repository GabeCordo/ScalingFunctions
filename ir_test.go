package plover

import (
	"errors"
	"testing"
)

func generateValidModuleIR() *ModuleIR {
	return &ModuleIR{
		Identifier: "common",
		Version:    "v0.1",
		Functions:  make([]FunctionIR, 0),
	}
}

func TestVerifyIR_Valid(t *testing.T) {

	ir := generateValidModuleIR()

	ee := VerifyIR(ir)
	if len(ee) > 0 {
		t.Errorf("expected IR verification to succeed")
	}
}

func TestVerifyIR_MissingModuleIdentifier(t *testing.T) {

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

func TestVerifyIR_MissingModuleVersion(t *testing.T) {

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

func TestVerifyIR_ValidModuleVersionVFormat(t *testing.T) {

	ir := generateValidModuleIR()
	ir.Version = "v0.1"

	ee := VerifyIR(ir)
	if len(ee) > 0 {
		t.Errorf("expected IR verification to pass")
		return
	}
}

func TestVerifyIR_ValidModuleVersionFloatFormat(t *testing.T) {

	ir := generateValidModuleIR()
	ir.Version = "v0.1"

	ee := VerifyIR(ir)
	if len(ee) > 0 {
		t.Errorf("expected IR verification to pass")
		return
	}
}

func TestVerifyIR_FunctionIdentifierValid(t *testing.T) {
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

func TestVerifyIR_FunctionIdentifierConflict(t *testing.T) {
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
