// Package ScalingFunctions
//
// Copyright (c) 2024-2026. Gabriel Cordovado
// All rights reserved.
//
// Source file:  repository.go
package ScalingFunctions

import (
	"errors"
	"sync"
)

const defaultModuleVersion = "v0.0.1"

var FunctionInModuleExistsErr = errors.New("module already has a function with this name")

type Module struct {
	Name      string
	Version   string
	functions map[string]F
	mutex     sync.RWMutex
}

func (module *Module) LinkFunction(name string, value any) error {

	module.mutex.Lock()
	defer module.mutex.Unlock()

	if _, found := module.functions[name]; found {
		return FunctionInModuleExistsErr
	} else {
		module.functions[name] = F{Id: name, Value: value}
	}

	return nil
}

func (module *Module) Map(mappings map[string]any) error {

	for name, value := range mappings {
		if err := module.LinkFunction(name, value); err != nil {
			return err
		}
	}
	return nil
}

func (module *Module) GetIR() *ModuleIR {

	ir := new(ModuleIR)
	ir.Identifier = module.Name
	ir.Version = module.Version

	numOfFunctions := len(module.functions)
	ir.Functions = make([]FunctionIR, numOfFunctions)

	idx := 0
	for _, function := range module.functions {
		ir.Functions[idx] = function.GetIR()
		idx++
	}

	return ir
}

type Repository struct {
	modules map[string]*Module
	mutex   sync.RWMutex
}

func NewRepository() *Repository {

	repository := new(Repository)
	repository.modules = make(map[string]*Module)
	return repository
}

// Module
// Find or create a new module to encapsulate clusters within. A module can be
// described as a set of clusters that relate in terms of functionality.
func (r *Repository) Module(name string) *Module {

	r.mutex.Lock()
	defer r.mutex.Unlock()

	if _, found := r.modules[name]; !found {

		mod := new(Module)
		mod.Name = name
		mod.Version = defaultModuleVersion
		mod.functions = make(map[string]F)

		r.modules[name] = mod
	}

	return r.modules[name]
}

// GetModules
// Find the modules registered to the Repository.
func (r *Repository) GetModules() (modules []*Module) {

	modules = make([]*Module, len(r.modules))

	idx := 0
	for _, m := range r.modules {
		modules[idx] = m
		idx++
	}

	return modules
}
