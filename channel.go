// Package ScalingFunctions
//
// Copyright (c) 2024-2026. Gabriel Cordovado
// All rights reserved.
//
// Source file:  channel.go
package ScalingFunctions

import (
	"errors"
	"reflect"
	"sync"
	"time"
)

////////////////////////////////////////////////////////////////////////
//						  Channel Constants
////////////////////////////////////////////////////////////////////////

// defaultQueueSize
// represents the pre allocated size of channels upon initialization.
const defaultQueueSize = 10000

////////////////////////////////////////////////////////////////////////
//							Channel Types
////////////////////////////////////////////////////////////////////////

// channelStatus
// represents the cStatus of the channel.
type channelStatus int

const (
	Empty channelStatus = iota
	Idle
	Healthy
	Underutilized
	Congested
	Closed
)

// ToString
// converts channelStatus into a human-readable string.
func (status channelStatus) ToString() string {

	switch status {
	case Idle:
		return "Idle"
	case Empty:
		return "Empty"
	case Congested:
		return "Congested"
	default:
		return "Healthy"
	}
}

type managedChannelConfig struct {
	Threshold              uint32
	UnderutilizedThreshold uint32
	GrowthFactor           float64
}

type channelDataWrapper struct {
	In   time.Time
	Data []reflect.Value
}

func (w channelDataWrapper) IsInvalid() bool {
	return w.In.IsZero() || w.Data == nil
}

type managedChannel struct {
	Name string

	cStatus channelStatus
	cSize   uint32

	Config managedChannelConfig

	Statistics             *TimingStatistics
	LastPush               time.Time
	UnderutilizedThreshold uint32
	TotalProcessed         uint32
	NumOfProducers         uint32

	channel chan channelDataWrapper

	cFlags struct {
		stopNewPushes   bool
		channelFinished bool
	}

	mutex sync.RWMutex

	wg sync.WaitGroup
}

////////////////////////////////////////////////////////////////////////
//							Channel Functions
////////////////////////////////////////////////////////////////////////

func newManagedChannel(name string, threshold uint32, growth float64, stats *TimingStatistics) (*managedChannel, error) {

	// The TimingStatistics pointer must not be nil.
	if stats == nil {
		return nil, errors.New("newManagedChannel passed nil pointer for *TimingStatistics")
	}

	// The threshold value must be greater or equal to 1
	if threshold < 1 {
		return nil, errors.New("newManagedChannel received a threshold less than 1")
	}

	// The growth value must be greater than 1.0
	if growth <= 1.0 {
		return nil, errors.New("newManagedChannel received a growth less or equal to 1.0")
	}

	mc := new(managedChannel)

	mc.Name = name
	mc.Config.Threshold = threshold
	mc.Config.GrowthFactor = growth
	mc.TotalProcessed = 0

	// allocate size(channelDataWrapper) * defaultQueueSize in advance to accommodate
	// the incoming data to the channel
	mc.channel = make(chan channelDataWrapper, defaultQueueSize)
	mc.Config.UnderutilizedThreshold = mc.Config.Threshold / 3
	mc.Statistics = stats

	mc.cFlags.channelFinished = false
	mc.cFlags.stopNewPushes = false
	mc.NumOfProducers = 0

	return mc, nil
}

func (mc *managedChannel) Push(data []reflect.Value) bool {

	mc.mutex.Lock()

	// see if we are hitting a threshold and the successive function is
	// getting overloaded with data units
	if (mc.cSize + 1) >= mc.Config.Threshold {
		mc.cStatus = Congested
	}

	mc.cSize++
	mc.TotalProcessed++

	currentTime := time.Now()
	mc.LastPush = currentTime

	stop := mc.cFlags.stopNewPushes
	mc.mutex.Unlock()

	if data == nil || stop {
		return false
	}

	mc.channel <- channelDataWrapper{In: currentTime, Data: data}

	return true
}

func (mc *managedChannel) DataPopped(timeIntoQueue time.Time) {

	mc.mutex.Lock()
	defer mc.mutex.Unlock()

	timeOutOfQueue := time.Now()
	totalTimeInQueue := timeOutOfQueue.Sub(timeIntoQueue)

	if mc.Statistics.AverageTime != 0 {
		if totalTimeInQueue > mc.Statistics.MaxTimeBeforePop {
			mc.Statistics.MaxTimeBeforePop = totalTimeInQueue
		} else if totalTimeInQueue < mc.Statistics.MinTimeBeforePop {
			mc.Statistics.MinTimeBeforePop = totalTimeInQueue
		}
		mc.Statistics.AverageTime = (mc.Statistics.AverageTime + totalTimeInQueue) / 2
	} else {
		mc.Statistics.AverageTime = totalTimeInQueue
		mc.Statistics.MedianTime = 0 // TODO: support
		mc.Statistics.MaxTimeBeforePop = totalTimeInQueue
		mc.Statistics.MinTimeBeforePop = totalTimeInQueue
	}

	if mc.cSize > 0 {
		mc.cSize--
	}
	mc.getStateLocked()
}

func (mc *managedChannel) Accepting() bool {

	mc.mutex.RLock()
	defer mc.mutex.RUnlock()

	return !mc.cFlags.stopNewPushes
}

func (mc *managedChannel) StopPushes() {

	mc.mutex.Lock()
	defer mc.mutex.Unlock()

	mc.cFlags.stopNewPushes = true
}

func (mc *managedChannel) AddProducer() {

	mc.mutex.Lock()
	defer mc.mutex.Unlock()

	mc.NumOfProducers++
}

func (mc *managedChannel) ProducerDone() error {

	mc.mutex.Lock()
	defer mc.mutex.Unlock()

	// Terminate the function if the call is invalid.
	if mc.NumOfProducers <= 0 {
		return errors.New("cannot call ProducerDone() when there are no producers")
	}

	mc.NumOfProducers--
	if !mc.cFlags.channelFinished && (mc.NumOfProducers <= 0) {
		mc.cFlags.channelFinished = true
		mc.cFlags.stopNewPushes = true
		close(mc.channel)
	}

	return nil
}

func (mc *managedChannel) getStateLocked() channelStatus {

	if mc.cFlags.channelFinished {
		mc.cStatus = Closed
	} else if mc.cSize == 0 {
		if time.Now().Sub(mc.LastPush).Seconds() > 3 {
			mc.cStatus = Idle
		} else {
			mc.cStatus = Empty
		}
	} else if ((mc.cStatus == Congested) || (mc.cStatus == Idle)) && (mc.cSize < mc.Config.UnderutilizedThreshold) {
		mc.cStatus = Underutilized
	} else if mc.cSize > mc.Config.Threshold {
		mc.cStatus = Congested
	} else {
		mc.cStatus = Healthy
	}

	return mc.cStatus
}

func (mc *managedChannel) GetState() channelStatus {

	mc.mutex.Lock()
	defer mc.mutex.Unlock()

	return mc.getStateLocked()
}
