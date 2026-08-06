// Package ScalingFunctions
//
// Copyright (c) 2026. Gabriel Cordovado
// All rights reserved.
//
// Source file:  builders_test.go

package builders

import (
	"testing"
	"time"
)

// Test_CreateGeneratorFunction
// test confirms that the generator function returned by `CreateGeneratorFunction`
// to generate a fixed number of packets and take a duration that equals the number
// of packets multiplied by the duration per packet.
func Test_CreateGeneratorFunction(t *testing.T) {

	duration := 1 * time.Millisecond
	numOfPackets := 10

	f := CreateGeneratorFunction(duration, numOfPackets)

	c := make(chan int)

	timeBeforeFuncStarted := time.Now()

	go f(c)

	numOfPacketsReceived := 0
	for range c {
		numOfPacketsReceived++
	}

	timeWhenFuncStopped := time.Now()

	if numOfPacketsReceived != numOfPackets {
		t.Errorf("expected %d packets but received only %d\n", numOfPackets, numOfPacketsReceived)
	}

	if timeWhenFuncStopped.Sub(timeBeforeFuncStarted) < (time.Duration(numOfPackets) * duration) {
		t.Errorf("expected the function to take at least %d\n", time.Duration(numOfPackets)*duration)
	}
}

// Test_CreateProcessorFunction
// test confirms that the processor function takes at least the specified duration.
func Test_CreateProcessorFunction(t *testing.T) {

	duration := 1 * time.Millisecond
	f := CreateProcessorFunction(duration)

	timeBeforeFuncStarted := time.Now()
	in := 0
	out, err := f(in)
	timeWhenFuncStopped := time.Now()

	if out != in {
		t.Error("expected the `in` and `out` values to match")
	}

	if err != nil {
		t.Error("expected a nil return value")
	}

	if timeWhenFuncStopped.Sub(timeBeforeFuncStarted) < duration {
		t.Errorf("expected the function to take at least %d\n", duration)
	}
}

// Test_CreateLoadFunction
// test confirms that the processor function takes at least the specified duration.
func Test_CreateLoadFunction(t *testing.T) {

	duration := 1 * time.Millisecond
	f := CreateLoadFunction(duration)

	timeBeforeFuncStarted := time.Now()
	err := f(0)
	timeWhenFuncStopped := time.Now()

	if err != nil {
		t.Error("expected a nil return value")
	}

	if timeWhenFuncStopped.Sub(timeBeforeFuncStarted) < duration {
		t.Errorf("expected the function to take at least %d\n", duration)
	}
}
