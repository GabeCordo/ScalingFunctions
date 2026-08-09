// Package ScalingFunctions
//
// Copyright (c) 2024-2026. Gabriel Cordovado
// All rights reserved.
//
// Source file:  repository_test.go
package ScalingFunctions

import (
	"errors"
	"fmt"
	"testing"
)

func addTwo(a, b int) int {
	return a + b
}

func print(a int) {
	fmt.Println(a)
}

////////////////////////////////////////////////////////////////////////
//
// Test  Module::LinkFunction( ... )
//	 ∟ Test_Module_LinkFunction_ValidFunction
//	 ∟ Test_Module_LinkFunction_DuplicateFunction
//
////////////////////////////////////////////////////////////////////////

func Test_Module_LinkFunction_ValidFunction(t *testing.T) {

	repository := NewRepository()
	if repository == nil {
		t.Error("received a nil Repository* pointer")
	}

	module := repository.Module("common")
	if module == nil {
		t.Error("received a nil Module* pointer")
	}

	genericId := "foo"
	genericFunc := func() {}

	err := module.LinkFunction(genericId, genericFunc)
	if err != nil {
		t.Error("expected a non-nil return type")
	}

	if len(module.functions) != 1 {
		t.Error("expected the number of functions in the module to be 1")
	}

	if module.functions[genericId].Id != genericId {
		t.Errorf("expected an internal mapping of '%s' inside the module\n", genericId)
	}
}

func Test_Module_LinkFunction_DuplicateFunction(t *testing.T) {

	repository := NewRepository()
	if repository == nil {
		t.Error("received a nil Repository* pointer")
	}

	module := repository.Module("common")
	if module == nil {
		t.Error("received a nil Module* pointer")
	}

	genericId := "foo"
	genericFunc := func() {}

	err := module.LinkFunction(genericId, genericFunc)
	if err != nil {
		t.Error("expected a non-nil return type")
	}

	err = module.LinkFunction(genericId, genericFunc)
	if !errors.Is(err, FunctionInModuleExistsErr) {
		t.Errorf("expected an err return type of type '%s'\n", FunctionInModuleExistsErr.Error())
	}

	if len(module.functions) != 1 {
		t.Error("expected the number of functions in the module to be 1")
	}

	if module.functions[genericId].Id != genericId {
		t.Errorf("expected an internal mapping of '%s' inside the module\n", genericId)
	}
}

////////////////////////////////////////////////////////////////////////
//
// Test  Module::GetIR( ... )
//	 ∟ Test_Module_GetIR
//
////////////////////////////////////////////////////////////////////////

func Test_Module_GetIR(t *testing.T) {

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

////////////////////////////////////////////////////////////////////////
//
// Test NewRepository( ... )
//	 ∟ Test_NewRepository
//
////////////////////////////////////////////////////////////////////////

func Test_NewRepository(t *testing.T) {

	repository := NewRepository()
	if repository == nil {
		t.Error("expected a valid Repository* returned")
	}

	if repository.modules == nil {
		t.Error("expected the Repository::modules field ot be intialized")
	}

	if len(repository.modules) != 0 {
		t.Error("expected the Repository::modules field to be initialized to a size of zero")
	}
}

////////////////////////////////////////////////////////////////////////
//
// Test Repository::Module( ... )
//	 ∟ Test_Repository_Module_VerifyDefaultFields
//
////////////////////////////////////////////////////////////////////////

func Test_Repository_Module_VerifyDefaultFields(t *testing.T) {

	repository := NewRepository()
	if repository == nil {
		t.Error("expected a valid Repository* returned")
	}

	moduleName := "common"
	commonModule := repository.Module(moduleName)
	if commonModule == nil {
		t.Error("expected a valid Module* returned")
	}

	if commonModule.Name != moduleName {
		t.Errorf("expected the module name '%s' but received '%s'\n",
			moduleName, commonModule.Name)
	}

	if commonModule.Version != defaultModuleVersion {
		t.Errorf("expected the module version '%s' but received '%s'\n",
			defaultModuleVersion, commonModule.Version)
	}

	if commonModule.functions == nil {
		t.Error("expected Module::functions to be initialized")
	}

	if len(commonModule.functions) != 0 {
		t.Error("expected Module::functinos to be initialized to a size of zero")
	}

	if repository.modules[moduleName] != commonModule {
		t.Error("expected an internal mapping in repository from name -> object")
	}
}

////////////////////////////////////////////////////////////////////////
//
// Test Repository::GetModules( ... )
//	 ∟ Test_Repository_GetModules_ValidateNumOfModules
//
////////////////////////////////////////////////////////////////////////

func Test_Repository_GetModules_ValidateNumOfModules(t *testing.T) {

	repository := NewRepository()
	if repository == nil {
		t.Error("expected a valid Repository* returned")
	}

	m1 := repository.Module("foo")
	if m1 == nil {
		t.Error("expected a valid Module* returned")
	}

	m2 := repository.Module("bar")
	if m2 == nil {
		t.Error("expected a valid Module* returned")
	}

	mm := repository.GetModules()

	if mm == nil {
		t.Error("expected a valid array returned from GetModules()")
	}

	if len(mm) != 2 {
		t.Error("expected an array of size 2 from GetModules()")
	}

	for _, m := range mm {
		if (m.Name != m1.Name) && (m.Name != m2.Name) {
			t.Error("GetModules() returned a module that is unknown")
		}
	}
}
