package yule

import (
	"reflect"
	"sync"
	"time"
)

// defaultQueueSize
// represents the pre allocated size of channels upon initialization.
const defaultQueueSize = 10000

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
	Threshold              int
	UnderutilizedThreshold int
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
	cSize   int

	Config managedChannelConfig

	Statistics             *TimingStatistics
	LastPush               time.Time
	UnderutilizedThreshold int
	TotalProcessed         int
	NumOfProducers         int

	channel chan channelDataWrapper

	cFlags struct {
		stopNewPushes   bool
		channelFinished bool
	}

	cMutexes struct {
		producer sync.RWMutex
		size     sync.Mutex
	}

	wg sync.WaitGroup
}

func newManagedChannel(name string, threshold int, growth float64, stats *TimingStatistics) *managedChannel {

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

	return mc
}

func (mc *managedChannel) Push(data []reflect.Value) bool {

	mc.cMutexes.size.Lock()

	// don't push to the channel if it is supposed to be closed
	if mc.cFlags.stopNewPushes {
		return false
	}

	// see if we are hitting a threshold and the successive function is
	// getting overloaded with data units
	if (mc.cSize + 1) >= mc.Config.Threshold {
		mc.cStatus = Congested
	}

	mc.cSize++
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
	mc.cMutexes.size.Unlock()

	if data == nil {
		return false
	}

	mc.channel <- channelDataWrapper{In: currentTime, Data: data}

	return true
}

func (mc *managedChannel) DataPopped(timeIntoQueue time.Time) {

	mc.cMutexes.size.Lock()
	defer mc.cMutexes.size.Unlock()

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

	mc.cSize--
	mc.cStatus = mc.GetState()
}

func (mc *managedChannel) Accepting() bool {
	return !mc.cFlags.stopNewPushes
}

func (mc *managedChannel) StopPushes() {
	mc.cFlags.stopNewPushes = true
}

func (mc *managedChannel) AddProducer() {
	mc.cMutexes.producer.Lock()
	defer mc.cMutexes.producer.Unlock()

	mc.NumOfProducers++
}

func (mc *managedChannel) ProducerDone() {

	mc.cMutexes.producer.Lock()
	defer mc.cMutexes.producer.Unlock()

	mc.NumOfProducers--

	if !mc.cFlags.channelFinished && (mc.NumOfProducers <= 0) {
		mc.cFlags.channelFinished = true
		close(mc.channel)
	}
}

func (mc *managedChannel) GetState() channelStatus {

	mc.cMutexes.producer.RLock()
	defer mc.cMutexes.producer.RUnlock()

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
