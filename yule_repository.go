// Package yule
//
// Copyright (c) 2024-2025. Gabriel Cordovado
// All rights reserved.
//
// Source file:  yule_repository.go
package yule

import (
	"errors"
	"sync"
)

type Module struct {
	Name      string
	Version   string
	functions map[string]FunctionLink
	mutex     sync.RWMutex
}

func (module *Module) LinkFunction(name string, value any) error {

	module.mutex.Lock()
	defer module.mutex.Unlock()

	if _, found := module.functions[name]; found {
		return errors.New("module already has a function with this name")
	} else {
		module.functions[name] = FunctionLink{Id: name, Value: value}
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
		mod.functions = make(map[string]FunctionLink)

		r.modules[name] = mod
	}

	return r.modules[name]
}
