// Package ScalingFunctions
//
// Copyright (c) 2026. Gabriel Cordovado
// All rights reserved.
//
// Source file:  statistics_test.go
package ScalingFunctions

import (
	"testing"
)

func TestNewStatistics(t *testing.T) {

	var numOfFuncs uint16 = 1
	var numOfPipes uint16 = 1

	s := NewStatistics(numOfFuncs, numOfPipes)

	if (s.NumOfFunctions != numOfFuncs) || (len(s.Functions) != int(numOfFuncs)) {
		t.Errorf("expected s.NumOfFunctions to be %d but received %d\n", numOfFuncs, s.NumOfFunctions)
	}

	if (s.NumOfChannels != numOfPipes) || (len(s.Pipes) != int(numOfPipes)) {
		t.Errorf("expected s.NumOfChannels to be %d but received %d\n", numOfPipes, s.NumOfChannels)
	}
}

func TestStatisticPrint(t *testing.T) {

	i := Build(gen, mul, prt)
	i.Run()

	s := i.GetStatistics()
	s.Print()

}
