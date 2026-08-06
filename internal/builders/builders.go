// Package ScalingFunctions
//
// Copyright (c) 2026. Gabriel Cordovado
// All rights reserved.
//
// Source file:  builders.go

package builders

import (
	"time"
)

var NumberGenerated int = 10

// CreateGeneratorFunction
// returns a function that generates `numberOfPackets` with a `generationDelay` between packets.
func CreateGeneratorFunction(generationDelay time.Duration, numOfPackets int) func(chan int) {

	return func(out chan int) {
		for i := 0; i < numOfPackets; i++ {
			time.Sleep(generationDelay)
			out <- i
		}
		close(out) // TODO: the framework shoud have it's own timeout in case this is forgotten
	}
}

// CreateProcessorFunction
// returns a function that waits `processingDelay` before returning.
func CreateProcessorFunction(processingDelay time.Duration) func(int) (int, error) {

	return func(in int) (out int, err error) {
		time.Sleep(processingDelay)
		out = in
		return out, nil
	}
}

// CreateLoadFunction
// returns a function that waits `loadDelay` before returning.
func CreateLoadFunction(loadDelay time.Duration) func(int) error {

	return func(in int) (err error) {
		time.Sleep(loadDelay)
		return nil
	}
}
