// Package main
//
// Copyright (c) 2026. Gabriel Cordovado
// All rights reserved.
//
// Source file:  main.go
//
// Description: The three-consumers example demonstrates a pipeline with three consumers
//
//	and varrying drain rates.
package main

import (
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/GabeCordo/ScalingFunctions"
)

////////////////////////////////////////////////////////////////////////////////////////
//									Pipeline Functions
////////////////////////////////////////////////////////////////////////////////////////

func generator(out chan int) {
	for i := 0; i < 1000000; i++ {
		out <- i
	}
	close(out)
}

func stepOne(in int) (out int) {
	time.Sleep(1 * time.Microsecond)
	out = in
	return in
}

func stepTwo(in int) (out int) {
	time.Sleep(5 * time.Microsecond)
	out = in
	return in
}

func stepThree(in int) {
	time.Sleep(3 * time.Microsecond)
}

////////////////////////////////////////////////////////////////////////////////////////
//									  Framework Glue
////////////////////////////////////////////////////////////////////////////////////////

func main() {

	statistics := make([]ScalingFunctions.Statistics, 0)

	pipeline := ScalingFunctions.Build(generator, stepOne, stepTwo, stepThree)

	go func() {
		for {
			statistics = append(statistics, *pipeline.GetStatistics())
			time.Sleep(100 * time.Millisecond)
		}
	}()

	pipeline.Run()

	file, err := os.Create("three-consumers.json")
	if err != nil {
		log.Panic(err)
	}
	defer file.Close()

	// Encodes and writes directly to the open file descriptor
	err = json.NewEncoder(file).Encode(statistics)
	if err != nil {
		log.Panic(err)
	}
}
