package yule

import (
	"fmt"
	"reflect"
	"sync"
	"time"
)

type BadManagedChannelType struct {
	description string
}

func (bmce BadManagedChannelType) Error() string {
	return bmce.description
}

type channelStatus int

const (
	Empty channelStatus = iota
	Idle
	Healthy
	Underutilized
	Congested
	Closed
)

const QueueSize = 10000

type managedChannelConfig struct {
	Threshold              int
	UnderutilizedThreshold int
	GrowthFactor           float64
}

type wrapper struct {
	In   time.Time
	Data []reflect.Value
}

func (w wrapper) IsInvalid() bool {
	return w.In.IsZero() || w.Data == nil
}

type managedChannel struct {
	Name string

	State  channelStatus
	Size   int
	Config managedChannelConfig

	Statistics     *TimingStatistics
	TotalProcessed int

	channel chan wrapper

	LastPush               time.Time
	UnderutilizedThreshold int
	StopNewPushes          bool
	ChannelFinished        bool

	NumOfProducers int

	producerMux sync.RWMutex
	sizeMux     sync.Mutex

	wg sync.WaitGroup
}

func New(name string, threshold int, growth float64, stats *TimingStatistics) *managedChannel {
	mc := new(managedChannel)

	mc.Name = name
	mc.Config.Threshold = threshold
	mc.Config.GrowthFactor = growth
	mc.TotalProcessed = 0

	// allocate size(wrapper) * QueueSize in advance to accommodate
	// the incoming data to the channel
	mc.channel = make(chan wrapper, QueueSize)
	mc.Config.UnderutilizedThreshold = mc.Config.Threshold / 3
	mc.Statistics = stats

	mc.ChannelFinished = false
	mc.StopNewPushes = false
	mc.NumOfProducers = 0

	return mc
}

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

func (mc *managedChannel) GetChannel() chan wrapper {
	return mc.channel
}

func (mc *managedChannel) Push(data []reflect.Value) bool {

	mc.sizeMux.Lock()

	// don't push to the channel if it is supposed to be closed
	if mc.StopNewPushes {
		return false
	}

	// see if we are hitting a threshold and the successive function is
	// getting overloaded with data units
	if (mc.Size + 1) >= mc.Config.Threshold {
		mc.State = Congested
	}

	mc.Size++
	mc.TotalProcessed++

	currentTime := time.Now()
	mc.LastPush = currentTime

	// before writing on the channel we want to release the mutex otherwise
	// the managed channel can enter a deadlock;
	//
	// producer: wants to send more data onto the managedChannel but has to
	//			 wait until the channel has buffer to allow it
	//			 -> channel send is blocked
	//
	// consumer: wants to read data from the managedChannel and decrement the
	//			 size but it needs to enter the mutex to do that.
	//			 -> the mutex on DataPopped() will block waiting for the chance
	//				to write to the queue
	//				BUT
	//				since the producer is waiting to write to the channel and
	//				the consumer can't continue pulling data off the queue we
	//				enter an infinite deadlock!
	mc.sizeMux.Unlock()

	if data == nil {
		return false
	}

	mc.channel <- wrapper{In: currentTime, Data: data}

	return true
}

func (mc *managedChannel) DataPopped(timeIntoQueue time.Time) {

	mc.sizeMux.Lock()
	defer mc.sizeMux.Unlock()

	timeOutOfQueue := time.Now()
	totalTimeInQueue := timeOutOfQueue.Sub(timeIntoQueue)

	if mc.Statistics.AverageTime != 0 {
		if totalTimeInQueue > mc.Statistics.MaxTimeBeforePop {
			mc.Statistics.MaxTimeBeforePop = totalTimeInQueue
		} else if totalTimeInQueue < mc.Statistics.MinTimeBeforePop {
			mc.Statistics.MinTimeBeforePop = totalTimeInQueue
		}
		mc.Statistics.AverageTime += totalTimeInQueue / 2
	} else {
		mc.Statistics.AverageTime = totalTimeInQueue
		mc.Statistics.MedianTime = 0 // TODO: support
		mc.Statistics.MaxTimeBeforePop = totalTimeInQueue
		mc.Statistics.MinTimeBeforePop = totalTimeInQueue
	}

	mc.Size--
	mc.State = mc.GetState()
}

func (mc *managedChannel) Accepting() bool {
	return !mc.StopNewPushes
}

func (mc *managedChannel) StopPushes() {
	mc.StopNewPushes = true
}

func (mc *managedChannel) AddProducer() {
	mc.producerMux.Lock()
	defer mc.producerMux.Unlock()

	mc.NumOfProducers++
}

func (mc *managedChannel) ProducerDone() {

	mc.producerMux.Lock()
	defer mc.producerMux.Unlock()

	mc.NumOfProducers--

	if !mc.ChannelFinished && (mc.NumOfProducers <= 0) {
		mc.ChannelFinished = true
		close(mc.channel)
	}
}

func (mc *managedChannel) GetState() channelStatus {

	mc.producerMux.RLock()
	defer mc.producerMux.RUnlock()

	if mc.ChannelFinished {
		mc.State = Closed
	} else if mc.Size == 0 {
		if time.Now().Sub(mc.LastPush).Seconds() > 3 {
			mc.State = Idle
		} else {
			mc.State = Empty
		}
	} else if ((mc.State == Congested) || (mc.State == Idle)) && (mc.Size < mc.Config.UnderutilizedThreshold) {
		mc.State = Underutilized
	} else if mc.Size > mc.Config.Threshold {
		mc.State = Congested
	} else {
		mc.State = Healthy
	}

	return mc.State
}

func (mc *managedChannel) GetGrowthFactor() float64 {

	return mc.Config.GrowthFactor
}

func (mc *managedChannel) AmountOfDataSeen() int {
	return mc.TotalProcessed
}

func (mc *managedChannel) ToString() string {
	return fmt.Sprintf("[%s][%s][Size: %d]\n", mc.Name, mc.State.ToString(), mc.Size)
}
