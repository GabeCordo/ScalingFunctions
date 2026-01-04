package plover

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

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
