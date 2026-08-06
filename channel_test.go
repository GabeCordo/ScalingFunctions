// Package ScalingFunctions
//
// Copyright (c) 2024-2026. Gabriel Cordovado
// All rights reserved.
//
// Source file:  channel_test.go
package ScalingFunctions

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

////////////////////////////////////////////////////////////////////////
//
// Test  channelStatus::ToString( ... )
//	 ∟ TestChannelStatus_Idle
//	 ∟ TestChannelStatus_Empty
//	 ∟ TestChannelStatus_Congested
//	 ∟ TestChannelStatus_Healthy
//
////////////////////////////////////////////////////////////////////////

func TestChannelStatus_Idle(t *testing.T) {

	cs := Idle

	if cs.ToString() != "Idle" {
		t.Error("expected ToString() to return `Idle`")
	}
}

func TestChannelStatus_Empty(t *testing.T) {

	cs := Empty

	if cs.ToString() != "Empty" {
		t.Error("expected ToString() to return `Empty`")
	}
}

func TestChannelStatus_Congested(t *testing.T) {

	cs := Congested

	if cs.ToString() != "Congested" {
		t.Error("expected ToString() to return `Congested`")
	}
}

func TestChannelStatus_Healthy(t *testing.T) {

	cs := Healthy

	if cs.ToString() != "Healthy" {
		t.Error("expected ToString() to return `Healthy`")
	}
}

////////////////////////////////////////////////////////////////////////
//
// Test  newManagedChannel( ... )
//	 ∟ TestManagedChannel_NewManagedChannel_NilStats
//	 ∟ TestManagedChannel_NewManagedChannel_InvalidTheshold
//	 ∟ TestManagedChannel_NewManagedChannel_InvalidGrowth
//	 ∟ TestManagedChannel_NewManagedChannel_Valid
//
////////////////////////////////////////////////////////////////////////

func TestManagedChannel_NewManagedChannel_NilStats(t *testing.T) {

	// Expect the constructor to fail since the statistic pointer is nil.
	_, err := newManagedChannel("foo", 100, 2.0, nil)
	if err == nil {
		t.Error("expected the constructor to fail.")
	}
}

func TestManagedChannel_NewManagedChannel_InvalidTheshold(t *testing.T) {

	s := TimingStatistics{}
	// Expect the constructor to fail since the threshold is invalid.
	_, err := newManagedChannel("foo", 0, 2.0, &s)
	if err == nil {
		t.Error("expected the contructor to fail.")
	}
}

func TestManagedChannel_NewManagedChannel_InvalidGrowth(t *testing.T) {

	s := TimingStatistics{}
	// Expect the constructor to fail since the growth is invalid.
	_, err := newManagedChannel("foo", 1, 1.0, &s)
	if err == nil {
		t.Error("expected the constructor to fail.")
	}
}

func TestManagedChannel_NewManagedChannel_Valid(t *testing.T) {

	s := TimingStatistics{}
	mc, err := newManagedChannel("foo", 1, 2.0, &s)
	if err != nil {
		t.Error("expected the constructor to pass.")
	}

	if mc.cStatus != Empty {
		t.Error("expected the channel to be empty.")
	}

	if mc.cSize != 0 {
		t.Error("expected the size of the channel to be zero.")
	}

	if mc.NumOfProducers != 0 {
		t.Error("expected no producers to be on the managed channel.")
	}
}

////////////////////////////////////////////////////////////////////////
//
// Test  managedChannel::Push( ... )
//	 ∟ TestManagedChannel_Push_InvalidData
//	 ∟ TestManagedChannel_Push_StopPushes
//
////////////////////////////////////////////////////////////////////////

func TestManagedChannel_Push_InvalidData(t *testing.T) {

	s := TimingStatistics{}
	mc, err := newManagedChannel("foo", 1, 2.0, &s)
	if err != nil {
		t.Error("expected the constructor to pass.")
	}

	result := mc.Push(nil)
	if result {
		t.Error("expected the channel to fail when receiving a nil value")
	}
}

func TestManagedChannel_Push_StopPushes(t *testing.T) {

	s := TimingStatistics{}
	mc, err := newManagedChannel("foo", 1, 2.0, &s)
	if err != nil {
		t.Error("expected the constructor to pass.")
	}

	mc.StopPushes()

	result := mc.Push(nil)
	if result {
		t.Error("expected the channel to fail when receiving a nil value")
	}
}

////////////////////////////////////////////////////////////////////////
//
// Test  AddProducer( ... )
//	 ∟ TestManagedChannel_AddProducer_Valid
//
////////////////////////////////////////////////////////////////////////

func TestManagedChannel_AddProducer_Valid(t *testing.T) {

	s := TimingStatistics{}
	mc, err := newManagedChannel("foo", 1, 2.0, &s)
	if err != nil {
		t.Skip()
	}

	mc.AddProducer()

	if mc.NumOfProducers != 1 {
		t.Error("expected the number of producers to be 1.")
	}

	mc.ProducerDone()

	if mc.NumOfProducers != 0 {
		t.Error("expected the number of producers to be 0.")
	}
}

////////////////////////////////////////////////////////////////////////
//
// Test  ProducerDone( ... )
//	 ∟ TestManagedChannel_ProducerDone_Invalid
//
////////////////////////////////////////////////////////////////////////

func TestManagedChannel_ProducerDone_Invalid(t *testing.T) {

	s := TimingStatistics{}
	mc, err := newManagedChannel("foo", 1, 2.0, &s)
	if err != nil {
		t.Skip()
	}

	mc.ProducerDone()

	if mc.NumOfProducers != 0 {
		t.Error("expected ProducerDone() to ignore the invalid call.")
	}
}

////////////////////////////////////////////////////////////////////////
//
// Test  managedChannel::GetState( ... )
//	 ∟ TestManagedChannel_GetState_Underutilized
//	 ∟ TestManagedChannel_GetState_Healthy
//
////////////////////////////////////////////////////////////////////////

func TestManagedChannel_GetState_Underutilized(t *testing.T) {

	s := TimingStatistics{}
	mc, err := newManagedChannel("foo", 10, 2.0, &s)
	if err != nil {
		t.Skip()
	}

	mc.cStatus = Idle
	mc.cSize = 2 // room for 8 remaining packets

	if mc.GetState() != Underutilized {
		t.Error("expected the state to be underutilized")
	}
}

func TestManagedChannel_GetState_Healthy(t *testing.T) {

	s := TimingStatistics{}
	mc, err := newManagedChannel("foo", 10, 2.0, &s)
	if err != nil {
		t.Skip()
	}

	mc.cSize = 1 // room for 9 remaining packets

	if mc.GetState() != Healthy {
		t.Error("expected the state to be underutilized")
	}
}

////////////////////////////////////////////////////////////////////////
//
// Test  managedChannel::Statistics
//	 ∟ TestManagedChannel_DataStatistic
//
////////////////////////////////////////////////////////////////////////

func TestManagedChannel_DataStatistic(t *testing.T) {

	s := TimingStatistics{}
	mc, err := newManagedChannel("foo", 1, 2.0, &s)
	if err != nil {
		t.Skip()
	}

	v := reflect.ValueOf(1)
	vv := []reflect.Value{v}

	mc.AddProducer()

	for i := 0; i < 10; i++ {
		mc.Push(vv)
		time.Sleep(1 * time.Millisecond)
	}
	err = mc.ProducerDone()
	if err != nil {
		t.Error(err)
		return
	}

	for data := range mc.channel {
		mc.DataPopped(data.In)
	}

	fmt.Println(mc.Statistics.AverageTime)
	fmt.Println(mc.Statistics.MaxTimeBeforePop)
	fmt.Println(mc.Statistics.MinTimeBeforePop)
	fmt.Println(mc.Statistics.MedianTime)
}

////////////////////////////////////////////////////////////////////////
//
// Test  managedChannel::Accepting
//	 ∟ TestManagedChannel_Accepting
//
////////////////////////////////////////////////////////////////////////

func TestManagedChannel_Accepting(t *testing.T) {

	s := TimingStatistics{}
	mc, err := newManagedChannel("foo", 32, 2.0, &s)

	if err != nil {
		t.Errorf("expected `err` to be `nil` but received `%s`\n", err.Error())
		return
	}

	if !mc.Accepting() {
		t.Error("expected the managed channel to be accepting")
		return
	}
}

////////////////////////////////////////////////////////////////////////
//
// Test  managedChannel::StopPushes
//	 ∟ TestManagedChannel_StopPushes
//
////////////////////////////////////////////////////////////////////////

func TestManagedChannel_StopPushes(t *testing.T) {

	s := TimingStatistics{}
	mc, err := newManagedChannel("foo", 32, 2.0, &s)

	if err != nil {
		t.Errorf("expected `err` to be `nil` but received `%s`\n", err.Error())
		return
	}

	mc.StopPushes()

	if mc.Accepting() {
		t.Error("expected the managed channel to be rejecting new messages")
		return
	}
}
