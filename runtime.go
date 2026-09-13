// Package ScalingFunctions
//
// Copyright (c) 2024-2026. Gabriel Cordovado
// All rights reserved.
//
// Source file:  runtime.go
package ScalingFunctions

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"

	"github.com/GabeCordo/ScalingFunctions/internal/flags"
)

////////////////////////////////////////////////////////////////////////////////
////							Constants									////
////////////////////////////////////////////////////////////////////////////////

const (
	defaultMonitorRefreshDuration = 100
)

////////////////////////////////////////////////////////////////////////////////
////								Errors									////
////////////////////////////////////////////////////////////////////////////////

var FunctionHasZeroStartWith = errors.New("pFunction cannot have a StartWith config of zero")

////////////////////////////////////////////////////////////////////////////////
////								Types									////
////////////////////////////////////////////////////////////////////////////////

// RunStatus
// represents the current cStatus of the pipeline.
type RunStatus string

const (
	UnTouched    RunStatus = "untouched"
	Starting               = "starting"
	Active                 = "active"
	Provisioning           = "provisioning"
	Failed                 = "failed"
	Stopping               = "stopping"
	Terminated             = "terminated"
	Unknown                = "-"
)

func (status RunStatus) ToString() string {
	switch status {
	case UnTouched:
		return "UnTouched"
	case Starting:
		return "Setup"
	case Active:
		return "Active"
	case Provisioning:
		return "Provisioning"
	case Failed:
		return "Failed"
	case Terminated:
		return "Terminated"
	default:
		return "None"
	}
}

// runEvent
// represents an event that can be pushed to the pipeline.
type runEvent uint8

const (
	Startup runEvent = iota
	Ready
	StartProvision
	EndProvision
	Error
	Suspend
	TearedDown
	StartReport
	EndReport
)

// functionTerminationCause
// represents the reason function execution was stopped.
type functionTerminationCause uint8

const (
	Complete functionTerminationCause = iota
	Forced
)

////////////////////////////////////////////////////////////////////////////////
////						   pipelineRuntime								////
////////////////////////////////////////////////////////////////////////////////

// pipelineRuntime
// is a container that holds all values used by a running instance of a Pipeline.
type pipelineRuntime struct {
	Id     uint64    `json:"id"`     // a unique identifier for the pipeline instance.
	Status RunStatus `json:"status"` // the cStatus of the pipeline

	Pipeline Pipeline // each pipeline has zero to many pipelineRuntime instances

	injectables      []reflect.Value
	numOfInjectables int

	testing       bool
	testingReport TestReport

	waitGroup struct {
		startup sync.WaitGroup // completes after the startup procedure has completed
	}

	mutex struct {
		global         sync.RWMutex // ensures that the cStatus machine is treated as a critical section
		threadCreation sync.Mutex   // ensure that thread creations is treated as a critical section
	}
}

// newPipelineRuntime
// builds a pipelineRuntime
func newPipelineRuntime(pipeline Pipeline) *pipelineRuntime {
	supervisor := new(pipelineRuntime)

	/**
	 * Note: we may wish to dynamically modify the threshold and growth-factor rates
	 *       used by the managed channels to vary how provisioning of new transform and
	 *       load goroutines are created. This allows us to create an autonomous system
	 *       that "self improves" if the output of the monitor is looped back
	 */

	supervisor.Status = UnTouched

	supervisor.Pipeline = pipeline

	supervisor.testing = false

	supervisor.numOfInjectables = len(supervisor.injectables)
	supervisor.waitGroup.startup.Add(1)

	return supervisor
}

func (instance *pipelineRuntime) injectDependencies(injectables ...any) {

	instance.injectables = make([]reflect.Value, len(injectables))
	for idx, injectable := range injectables {
		instance.injectables[idx] = reflect.ValueOf(injectable)
	}
}

func (instance *pipelineRuntime) isForTesting() {
	instance.testing = true
}

func (instance *pipelineRuntime) startup() error {

	// the startup function may optionally be provided by the developer.
	if instance.Pipeline.onStartup != nil {
		if flags.DEBUG {
			log.Println("Running onStartup function")
		}

		f := reflect.ValueOf(instance.Pipeline.onStartup.Value)
		f.Call(instance.injectables)
	}

	// provision each function in the pipeline that is required before
	// data can begin flowing between functions in the pipeline.
	for _, function := range instance.Pipeline.functions {

		// If there exists a pFunction.Config.StartWith config that is 0,
		// it is possible for the pipeline to deadlock. One of the functions
		// in the pipeline will not be created breaking the chain of execution.
		//
		// (Example)
		// Function:		A	->	B	->	C	-> 	D
		// StartWith:		1		0		1		1
		//
		// pFunction 'B' has a StartWith 0 so the startup() function will not
		// start any listener for the 1st channel, or producer for the 2nd channel.
		//
		// pFunction 'A' will be pushing to a channel that isn't read.
		// pFunction 'C' and 'D' will be not receive data wasting resources.
		if function.Config.StartWith == 0 {
			return FunctionHasZeroStartWith
		}

		for j := uint16(0); (j < function.Config.StartWith) && (function.Config.Maximum == 0 || j < function.Config.Maximum); j++ {
			instance.provision(function)

			// note: these statistics are not run in parallel
			//		 ~ there is not risk of a data race
			function.Stats.Active++
			function.Stats.Provisions++
		}
	}

	return nil
}

// runtime
// every N seconds we should check if the ETChannel or TLChannel is congested
// and requires us to provision additional nodes.
func (instance *pipelineRuntime) runtime() {

	for {
		// if the pipeline has been terminated we should end the
		// runtime loop of checking the channels and scaling functions
		if instance.Status == Terminated {
			break
		}

		numOfClosedChannels := 0
		for _, chn := range instance.Pipeline.channels {

			channelState := chn.Value.GetState()

			if (instance.Status == Stopping) && chn.Value.Accepting() {
				chn.Value.StopPushes()
			}

			if channelState == Congested {

				chn.Stats.Breaches++

				for _, f := range chn.Receiver {
					n := chn.Config.GrowthFactor
					for (n > 0) && (f.Stats.Active < f.Config.Maximum) {
						f.Mutex.Lock()
						f.Stats.Provisions++
						f.Stats.Active++
						f.Mutex.Unlock()
						instance.provision(f)
						n--
					}
				}
			} else if (channelState == Underutilized) || (channelState == Idle) {

				for _, f := range chn.Receiver {

					n := chn.Config.GrowthFactor
					for n > 0 {
						f.Mutex.RLock()
						// never remove all transform nodes otherwise we risk the
						// ET channel having no consumers
						if f.Stats.Active <= 1 {
							f.Mutex.RUnlock()
							break
						}
						f.Mutex.RUnlock()
						f.Stats.Active--
						instance.remove(f)
						n--
					}
				}
			} else if channelState == Closed {
				numOfClosedChannels++
			}
		}

		// when all channels have been closed there is no reason
		// to keep the execution loop running.
		if numOfClosedChannels == len(instance.Pipeline.channels) {
			break
		}

		// check if the channel is congested after defaultMonitorRefreshDuration seconds
		time.Sleep(defaultMonitorRefreshDuration * time.Millisecond)
	}
}

func (instance *pipelineRuntime) teardown() {

	// TODO : add a guard in case this value is not a function
	if instance.Pipeline.onTeardown != nil {
		f := reflect.ValueOf(instance.Pipeline.onTeardown.Value)
		f.Call(instance.injectables)
	}

	instance.event(Suspend)
}

func (instance *pipelineRuntime) start() error {
	instance.event(Startup)
	defer instance.event(TearedDown)

	if flags.DEBUG {
		log.Printf("Starting instance with id: %d", instance.Id)
	}

	var err error = nil

	defer func() {
		// has the user defined function crashed during runtime?
		if r := recover(); r != nil {
			// yes => return a response that identifies that the cluster crashed
			err = errors.New("crashed during runtime") // TODO : make into standard error
		}
	}()

	//// start creating the default frontend goroutines

	err = instance.startup()
	if err != nil {
		instance.waitGroup.startup.Done()
		return err
	}
	instance.waitGroup.startup.Done() // allow actions that need to wait for startup to begin

	//// end creating the default frontend goroutines

	instance.runtime()

	instance.teardown()

	return err
}

////////////////////////////////////////////////////////////////////////////////
////					pipelineRuntime cStatus Machine						////
////////////////////////////////////////////////////////////////////////////////

func (instance *pipelineRuntime) event(event runEvent) bool {
	instance.mutex.global.Lock()
	defer instance.mutex.global.Unlock()

	if instance.Status == UnTouched {
		if event == Startup {
			instance.Status = Active
		} else if (event == Suspend) || (event == TearedDown) {
			instance.Status = Stopping
		} else {
			return false
		}
	} else if instance.Status == Active {
		if event == StartProvision {
			instance.Status = Provisioning
		} else if event == Error {
			instance.Status = Failed
		} else if event == Suspend {
			instance.Status = Stopping
		} else if event == TearedDown {
			instance.Status = Terminated
		} else {
			return false
		}
	} else if instance.Status == Provisioning {
		if event == EndProvision {
			instance.Status = Active
		} else if event == Error {
			instance.Status = Failed
		} else if event == Suspend {
			instance.Status = Stopping
		} else {
			return false
		}
	} else if instance.Status == Stopping {
		if event == TearedDown {
			instance.Status = Terminated
		} else {
			return false
		}
	} else if (instance.Status == Failed) || (instance.Status == Terminated) {
		return false
	}

	return true // represents a boolean ~ hasStateChanged?
}

func (instance *pipelineRuntime) IsAlive() bool {

	instance.mutex.global.RLock()
	defer instance.mutex.global.RUnlock()

	return (instance.Status != Failed) && (instance.Status != Terminated)
}

////////////////////////////////////////////////////////////////////////////////
////						   pipelineRuntime								////
////////////////////////////////////////////////////////////////////////////////

func (instance *pipelineRuntime) extractWrapper(function *pFunction, channel *managedChannel) <-chan struct{} {
	done := make(chan struct{})

	// the function always finishes till completion unless a direct shutdown is called on the server
	// which stops data collection from some source
	go func() {
		defer func() {
			done <- struct{}{}
			close(done)
		}()

		numberInjectables := len(instance.injectables)
		arguments := make([]reflect.Value, numberInjectables)
		copy(arguments, instance.injectables)

		// the first parameter of an extract function that is not an injectable
		// shall be a channel that the function can push extracted data to
		if (function.Reflected.Type.NumIn() - numberInjectables) > 0 {

			// do we expect to pass a pipe?
			channelType := function.Reflected.Type.In(numberInjectables)

			if channelType.Kind() == reflect.Chan {
				c := reflect.MakeChan(channelType, numberInjectables)
				arguments = append(arguments, c)
			}
		}

		go function.Reflected.Value.Call(arguments)

		// as the extract function runs asynchronously and sends data to the
		// channel, receive data from the channel and push data to the next
		// function in the Pipeline sequence.
		if (function.Reflected.Type.NumIn() - numberInjectables) > 0 {
			ch := arguments[numberInjectables]
			for {
				value, ok := ch.Recv()
				if ok {
					if !channel.Push([]reflect.Value{value}) {
						// Channel stopped accepting pushes. Drain ch in background so
						// the producer function doesn't deadlock sending to ch.
						go func(c reflect.Value) {
							for {
								_, ok := c.Recv()
								if !ok {
									break
								}
							}
						}(ch)
						break
					}
				} else {
					break
				}
			}
		}
	}()
	return done
}

func (instance *pipelineRuntime) extractShutdownWrapper() <-chan struct{} {
	done := make(chan struct{})

	// we need to create a separate goroutine otherwise it will block the current
	// thread from re-evaluating the select statement wherever the extractShutdownWrapper is called
	go func() {
		defer close(done)
		for {
			// the IsAlive clause ensures that once a runner is dead, we will not leak memory
			// with a forever-running goroutine
			instance.mutex.global.RLock()
			stopping := (instance.Status == Stopping) || (!instance.IsAlive())
			instance.mutex.global.RUnlock()

			if stopping {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	return done
}

var errorInterface = reflect.TypeOf((*error)(nil)).Elem()

// call
// a channelDataWrapper function that checks the returned values from a function call for
// error values. if the function returns an error that is non-nil we will set
// the returned boolean flag to true indicating something may have gone wrong
// inside the function call.
func (instance *pipelineRuntime) call(function *pFunction, ins []reflect.Value) ([]reflect.Value, bool) {

	drop := false

	// append the values given to the function on-top of the injectables that need to
	// be passed to every step in the Pipeline
	arguments := append(instance.injectables, ins...)
	results := function.Reflected.Value.Call(arguments)

	// TODO : the number of results returned by a function can be pre-computed
	numResults := len(results)

	// a common pattern in golang is to return (value, error) where error can be used
	// to identify that the function was not able to run to completion.
	//
	// the Pipeline supports a similar method of identifying that the function failed
	// to run successfully to completion. when the function returns an error value
	// (that must be the last value returned) the Pipeline will check whether the
	// error is nil or not to determine whether the resultant data should be used.
	//
	// error is nil -> keep sending the resultant data along the Pipeline
	// error -> do NOT pass the data along the Pipeline (aka. drop the data)
	if numResults > 0 {

		lastResult := results[numResults-1]

		isError := lastResult.Type().Implements(errorInterface)

		if isError {
			isNil := lastResult.IsNil()

			if !isNil {
				// the framework shall be responsible for displaying errors sent
				// by the Pipeline to avoid requiring the developer to handle and return
				// the error which is considered an anti-pattern
				log.Println(lastResult.Elem())

				if instance.testing {
					instance.testingReport.Success = false
					instance.testingReport.Step = function.Identifier
					instance.testingReport.Cause = lastResult.Interface().(error)
				}
				drop = true
			}

			// never pass an error along the Pipeline
			results = results[:numResults-1]
		}
	}

	return results, drop
}

// provision
// creates a new running instance of a pFunction.
func (instance *pipelineRuntime) provision(function *pFunction) {
	instance.event(StartProvision)
	defer instance.event(EndProvision)

	// the runner must provision new threads one at a time.
	// note: avoid the possibility of >1 thread modifying the wait-group at a time
	instance.mutex.threadCreation.Lock()
	defer instance.mutex.threadCreation.Unlock()

	// a new function is provisioned
	// we should inform the wait group that the runner isn't finished until the wg is done
	//instance.waitGroup.Add(1)

	go func(supervisor *pipelineRuntime, function *pFunction) {

		if (function.From == nil) && (function.To != nil) {
			// the function is a STARTING NODE of the Pipeline if no data is being received

			defer func() {
				if r := recover(); r != nil {
					out := fmt.Sprintf("cluster.Extract function raised error: %v", r)
					err := errors.New(out)
					if instance.testing {
						instance.testingReport.Success = false
						instance.testingReport.Step = function.Identifier
						instance.testingReport.Cause = err
					}
					log.Println(err)
					function.To.Value.ProducerDone()
					//supervisor.waitGroup.Done()
				}
			}()

			// support passing values to the Pipeline manually
			// todo : add more description
			if instance.testing {
				return
			}

			function.To.Value.AddProducer()

			if flags.DEBUG {
				log.Printf("creating new producer function %d\n", supervisor.Id)
			}

			// TODO : reword
			// if the producer function has output, then don't worry about spawning a channelDataWrapper,
			// allow the function to return normally and send the data along the pipe

			if function.Reflected.Type.NumOut() > 0 {

				output := function.Reflected.Value.Call(instance.injectables)
				function.To.Value.Push(output)

			} else {

				select {
				case <-supervisor.extractWrapper(function, function.To.Value):
					break
				case <-supervisor.extractShutdownWrapper():
					fmt.Println("shutdown caused extract to finish early")
					break
				}
			}

			function.Mutex.Lock()
			function.Stats.Active--
			function.Mutex.Unlock()

			// if the number of producers is 0, the ET channel will close that
			// allows the Transform goroutines to terminate once they have
			// completed processing all of their data
			function.To.Value.ProducerDone()
		} else if (function.From != nil) && (function.To == nil) {
			// the function is an ENDPOINT NODE of the Pipeline if no data is being sent

			quit := make(chan bool, 1)
			function.Mutex.Lock()
			function.Quit = append(function.Quit, quit)
			function.Mutex.Unlock()

			defer func() {
				if r := recover(); r != nil {
					out := fmt.Sprintf("cluster.Load function raised error: %v", r)
					err := errors.New(out)
					if instance.testing {
						instance.testingReport.Success = false
						instance.testingReport.Step = function.Identifier
						instance.testingReport.Cause = err
					}
					log.Println(err)
					//supervisor.waitGroup.Done()
				}
			}()

			if flags.DEBUG {
				log.Printf("creating new endpoint function %d\n", supervisor.Id)
			}

			var queuedRequests reflect.Value
			if function.Config.WaitBefore {
				in := function.Reflected.Type.In(0)
				queuedRequests = reflect.MakeSlice(in, 0, 0)
			}
			closeChan := false
			terminationCause := Complete

			for {
				select {
				case request := <-function.From.Value.channel:
					{
						if request.IsInvalid() {
							closeChan = true
							break
						}

						function.From.Mutex.Lock()
						function.From.Stats.Pulled++
						function.From.Mutex.Unlock()

						// associates a TimeOut to the data being removed from the channel and decrements
						// the data counter for the current pipe
						function.From.Value.DataPopped(request.In)

						// what: the developer has an option to wait for all the data to be received by a
						// channel before processing that data.
						//
						// why: we may need a bulk set of data before being able to do anything
						if function.Config.WaitBefore {
							queuedRequests = reflect.Append(queuedRequests, request.Data[0])
						} else {
							arguments := append(instance.injectables, request.Data...)
							function.Reflected.Value.Call(arguments)
						}
					}
				case <-quit:
					{
						terminationCause = Forced
						closeChan = true
					}
				}

				if closeChan {

					if terminationCause == Complete {
						function.Mutex.Lock()
						function.Stats.Active--
						function.Mutex.Unlock()
					}

					// if we were waiting for the channel to close before transforming the data,
					// call the function now that the channel is closed
					if function.Config.WaitBefore {
						arguments := append(instance.injectables, queuedRequests)
						function.Reflected.Value.Call(arguments)
					}

					break
				}
			}
		} else if (function.From != nil) && (function.To != nil) {
			defer func() {
				if r := recover(); r != nil {
					out := fmt.Sprintf("cluster.Transform function raised error: %v", r)
					err := errors.New(out)
					if instance.testing {
						instance.testingReport.Success = false
						instance.testingReport.Step = function.Identifier
						instance.testingReport.Cause = err
					}
					log.Println(err)
					function.To.Value.ProducerDone()
					//supervisor.waitGroup.Done()
				}
			}()

			quit := make(chan bool, 1)
			function.Mutex.Lock()
			function.Quit = append(function.Quit, quit)
			function.Mutex.Unlock()

			function.To.Value.AddProducer()

			if flags.DEBUG {
				log.Printf("creating new transformer function %d\n", supervisor.Id)
			}

			var queuedRequests reflect.Value
			if function.Config.WaitBefore {
				in := function.Reflected.Type.In(0)
				queuedRequests = reflect.MakeSlice(in, 0, 0)
			}
			closeChan := false
			terminationCause := Complete

			for {
				select {
				case request := <-function.From.Value.channel:
					{
						// sometimes we are receiving bad data?
						if request.IsInvalid() {
							closeChan = true
							break
						}

						// associates a TimeOut to the data being removed from the channel and decrements
						// the data counter for the current pipe
						function.From.Value.DataPopped(request.In)

						function.From.Mutex.Lock()
						function.From.Stats.Pulled++
						function.From.Mutex.Unlock()

						// what: the developer has an option to wait for all the data to be received by a
						// channel before processing that data.
						//
						// why: we may need a bulk set of data before being able to do anything
						if function.Config.WaitBefore {
							queuedRequests = reflect.Append(queuedRequests, request.Data[0])
						} else {
							results, drop := supervisor.call(function, request.Data)

							if !drop {
								isSliceExplosion := false
								if len(results) > 0 && results[0].Kind() == reflect.Slice {
									if len(function.To.Receiver) > 0 {
										nextFunction := function.To.Receiver[0]
										nextFunctionReflection := nextFunction.Reflected.Type
										if (nextFunctionReflection.NumIn() > instance.numOfInjectables) && (nextFunctionReflection.In(instance.numOfInjectables).Kind() != reflect.Slice) {
											isSliceExplosion = true
										}
									}
								}

								if isSliceExplosion {
									sliceLen := results[0].Len()
									function.To.Mutex.Lock()
									function.To.Stats.Pushed += uint64(sliceLen)
									function.To.Mutex.Unlock()

									for i := 0; i < sliceLen; i++ {
										function.To.Value.Push([]reflect.Value{results[0].Index(i)})
									}
								} else {
									function.To.Mutex.Lock()
									function.To.Stats.Pushed++
									function.To.Mutex.Unlock()

									function.To.Value.Push(results)
								}
							} else {
								function.From.Mutex.Lock()
								function.From.Stats.Dropped++
								function.From.Mutex.Unlock()
							}
						}
					}
				case <-quit:
					{
						terminationCause = Forced
						closeChan = true
					}
				}

				if closeChan {

					if terminationCause == Complete {
						function.Mutex.Lock()
						function.Stats.Active--
						function.Mutex.Unlock()
					}

					// if we were waiting for the channel to close before transforming the data,
					// call the function now that the channel is closed
					if function.Config.WaitBefore {
						results, drop := supervisor.call(function, []reflect.Value{queuedRequests})

						if !drop {
							function.To.Mutex.Lock()
							function.To.Stats.Pushed++
							function.To.Mutex.Unlock()

							function.To.Value.Push(results)
						} else {
							function.From.Mutex.Lock()
							function.From.Stats.Dropped++
							function.From.Mutex.Unlock()
						}
					}
					break
				}
			}

			// if the number of producers is 0, the TL channel will close that
			// allows the Load goroutines to terminate once they have
			// completed processing all of their data
			function.To.Value.ProducerDone()
		} else {
			panic("function is neither a producer or receiver of data")
		}

		// notify the wait group a process has completed ~ if all are finished we close the monitor
		//supervisor.waitGroup.Done()
	}(instance, function)
}

// remove
// terminates a running instance of type pFunction.
func (instance *pipelineRuntime) remove(function *pFunction) {

	function.Mutex.Lock()
	if function.Stats.Active <= 0 {
		function.Mutex.Unlock()
		panic("attempting to quit when no functions are running")
	}

	quit := function.Quit[0]
	function.Quit = function.Quit[1:]
	function.Mutex.Unlock()

	quit <- true
}

////////////////////////////////////////////////////////////////////////////////
////			functions to send data to a pipelineRuntime					////
////////////////////////////////////////////////////////////////////////////////

func (instance *pipelineRuntime) send(data any, functionId ...string) {

	specifiedFunction := len(functionId) > 0

	// the developer needs to specify which root they want to mimic
	// when the Pipeline's topology is more complex than a linear line
	if !specifiedFunction && (len(instance.Pipeline.roots) > 1) {
		panic("testing non-linear pipelines requires specifying which generator function is being stubbed")
	}

	instance.waitGroup.startup.Wait()
	instance.testingReport.Success = true

	isDataSent := false
	for _, f := range instance.Pipeline.roots {

		// do not push data to the Pipeline when the function identifier is
		// specified and does not match.
		if specifiedFunction && (f.Identifier != functionId[0]) {
			continue
		}
		f.To.Value.AddProducer()
		f.To.Value.Push([]reflect.Value{reflect.ValueOf(data)})

		isDataSent = true
	}

	if !isDataSent {
		panic("attempt to test the Pipeline failed, could not find a Pipeline to push data to")
	}
}

func (instance *pipelineRuntime) close() {

	instance.waitGroup.startup.Wait()

	if instance.testing {
		var err error
		for _, f := range instance.Pipeline.roots {
			f.Mutex.Lock()
			f.Stats.Active--
			f.Mutex.Unlock()

			err = f.To.Value.ProducerDone()
			if err != nil {
				fmt.Println(err)
			}
		}
	}

	instance.event(Suspend)

	for instance.IsAlive() {
		time.Sleep(10 * time.Millisecond)
	}
}

func (instance *pipelineRuntime) print() {
	fmt.Printf("Id: %d\n", instance.Id)
}
