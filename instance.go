package yule

import (
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"sync"
	"time"
)

const (
	defaultMonitorRefreshDuration = 100
)

type runStatus string

const (
	UnTouched    runStatus = "untouched"
	Starting               = "starting"
	Active                 = "active"
	Provisioning           = "provisioning"
	Failed                 = "failed"
	Stopping               = "stopping"
	Terminated             = "terminated"
	Unknown                = "-"
)

func (status runStatus) ToString() string {
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

type functionTerminationCause uint8

const (
	Complete functionTerminationCause = iota
	Forced
)

const MaximumRoutinesPerSupervisor = 2000

type runInstance struct {
	Id uint64 `json:"id"`

	State     runStatus `json:"status"`
	StartTime time.Time `json:"quitE-time"`

	Pipeline    *Pipeline
	metadata    map[string]string
	injectables []reflect.Value

	testing       bool
	testingReport TestReport

	startingWaitGroup sync.WaitGroup
	loadWaitGroup     sync.WaitGroup
	waitGroup         sync.WaitGroup
	threadMutex       sync.Mutex
	mutex             sync.RWMutex
}

func newInstance(pipeline *Pipeline, metadata map[string]string, injectables ...any) *runInstance {
	supervisor := new(runInstance)

	/**
	 * Note: we may wish to dynamically modify the threshold and growth-factor rates
	 *       used by the managed channels to vary how provisioning of new transform and
	 *       load goroutines are created. This allows us to create an autonomous system
	 *       that "self improves" if the output of the monitor is looped back
	 */

	supervisor.State = UnTouched

	supervisor.Pipeline = pipeline
	supervisor.metadata = metadata

	supervisor.injectables = make([]reflect.Value, len(injectables))
	for idx, injectable := range injectables {
		supervisor.injectables[idx] = reflect.ValueOf(injectable)
	}

	supervisor.startingWaitGroup.Add(1)

	return supervisor
}

type Summary struct {
	Namespace  string
	Pipeline   string
	Supervisor uint64
	Statistics *Statistics
}

func (instance *runInstance) Event(event runEvent) bool {
	instance.mutex.Lock()
	defer instance.mutex.Unlock()

	if instance.State == UnTouched {
		if event == Startup {
			instance.State = Active
		} else if (event == Suspend) || (event == TearedDown) {
			instance.State = Stopping
		} else {
			return false
		}
	} else if instance.State == Active {
		if event == StartProvision {
			instance.State = Provisioning
		} else if event == Error {
			instance.State = Failed
		} else if event == Suspend {
			instance.State = Stopping
		} else if event == TearedDown {
			instance.State = Terminated
		} else {
			return false
		}
	} else if instance.State == Provisioning {
		if event == EndProvision {
			instance.State = Active
		} else if event == Error {
			instance.State = Failed
		} else if event == Suspend {
			instance.State = Stopping
		} else {
			return false
		}
	} else if instance.State == Stopping {
		if event == TearedDown {
			instance.State = Terminated
		} else {
			return false
		}
	} else if (instance.State == Failed) || (instance.State == Terminated) {
		return false
	}

	return true // represents a boolean ~ hasStateChanged?
}

func (instance *runInstance) IsAlive() bool {

	instance.mutex.RLock()
	defer instance.mutex.RUnlock()

	return (instance.State != Failed) && (instance.State != Terminated)
}

func (instance *runInstance) Start(test bool) error {
	instance.Event(Startup)

	defer instance.Event(TearedDown)

	instance.testing = test

	if DEBUG {
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

	instance.StartTime = time.Now()

	if instance.Pipeline.OnStartup != nil {
		// TODO : add safety check here

		if DEBUG {
			log.Println("Running OnStartup function")
		}

		f := reflect.ValueOf(instance.Pipeline.OnStartup.Value)
		f.Call(instance.injectables)
	}

	//// add all the metadata passed to the pipeline to the local environment

	// TODO : possibly enhance security?
	for key, value := range instance.metadata {

		if DEBUG {
			log.Printf("added new environment value '%s'\n", key)
		}
		os.Setenv(key, value)
	}

	//// start creating the default frontend goroutines

	for _, function := range instance.Pipeline.Functions {

		for j := 0; (j < function.Config.StartWith) && (j < function.Config.Maximum); j++ {
			instance.Provision(function)
			function.Stats.Active++
			function.Stats.Provisions++
		}
	}

	//// end creating the default frontend goroutines

	// every N seconds we should check if the ETChannel or TLChannel is congested
	// and requires us to provision additional nodes
	go instance.Runtime()

	instance.startingWaitGroup.Done() // allow actions that need to wait for startup to begin
	instance.waitGroup.Wait()         // wait for the Extract-Transform-Load (ETL) Cycle to Complete

	// calculate the timings produced by data being fed across each of the channels
	// TODO: support
	//instance.CalculateTiming()

	//// cleanup environment variables that were dynamically set

	for key, _ := range instance.metadata {

		if DEBUG {
			log.Printf("deleted environment value '%s'\n", key)
		}
		os.Unsetenv(key)
	}

	return err
}

func (instance *runInstance) Teardown() {

	// TODO : add a guard in case this value is not a function
	if instance.Pipeline.OnStartup != nil {
		f := reflect.ValueOf(instance.Pipeline.OnTeardown.Value)
		f.Call(instance.injectables)
	}

	instance.Event(Suspend)
}

func (instance *runInstance) Runtime() {
	for {
		if instance.State == Terminated {
			break
		}

		for _, chn := range instance.Pipeline.Channels {

			channelState := chn.Value.GetState()

			if (instance.State == Stopping) && chn.Value.Accepting() {
				chn.Value.StopPushes()
			}

			if channelState == Congested {

				chn.Stats.Breaches++

				for _, f := range chn.Receiver {
					n := chn.Config.GrowthFactor
					for (n > 0) && (f.Stats.Active < f.Config.Maximum) {
						f.Stats.Provisions++
						f.Stats.Active++
						instance.Provision(f)
						n--
					}
				}
			} else if (channelState == Underutilized) || (channelState == Idle) {

				for _, f := range chn.Receiver {

					n := chn.Config.GrowthFactor
					for n > 0 {
						// never remove all transform nodes otherwise we risk the
						// ET channel having no consumers
						if f.Stats.Active <= 1 {
							break
						}
						f.Stats.Active--
						instance.Remove(f)
						n--
					}
				}
			}
		}

		// check if the channel is congested after defaultMonitorRefreshDuration seconds
		time.Sleep(defaultMonitorRefreshDuration * time.Millisecond)
	}
}

func (instance *runInstance) ExtractWrapper(function *pFunction, channel *managedChannel) <-chan struct{} {
	done := make(chan struct{})

	// the function always finishes till completion unless a direct shutdown is called on the server
	// which stops data collection from some source
	go func() {
		defer func() {
			done <- struct{}{}
			close(done)
		}()

		numOfInjectibles := len(instance.injectables)
		arguments := make([]reflect.Value, numOfInjectibles)
		copy(arguments, instance.injectables)

		// the first parameter of an extract function that is not an injectable
		// shall be a channel that the function can push extracted data to
		if (function.Reflected.Type.NumIn() - numOfInjectibles) > 0 {

			// do we expect to pass a pipe?
			channelType := function.Reflected.Type.In(numOfInjectibles)

			if channelType.Kind() == reflect.Chan {
				c := reflect.MakeChan(channelType, numOfInjectibles)
				arguments = append(arguments, c)
			}
		}

		go function.Reflected.Value.Call(arguments)

		// as the extract function runs asynchronously and sends data to the
		// channel, receive data from the channel and push data to the next
		// function in the pipeline sequence.
		if (function.Reflected.Type.NumIn() - numOfInjectibles) > 0 {

			for {
				value, ok := arguments[numOfInjectibles].Recv()
				if ok {
					channel.Push([]reflect.Value{value})
				} else {
					break
				}
			}
		}
	}()
	return done
}

func (instance *runInstance) ExtractShutdownWrapper() <-chan struct{} {
	done := make(chan struct{})

	// we need to create a separate goroutine otherwise it will block the current
	// thread from re-evaluating the select statement wherever the ExtractShutdownWrapper is called
	go func() {
		defer close(done)
		for {
			// the IsAlive clause ensures that once a runner is dead, we will not leak memory
			// with a forever-running goroutine
			if (instance.State == Stopping) || (!instance.IsAlive()) {
				break
			}
			time.Sleep(1 * time.Second)
		}
	}()

	return done
}

var errorInterface = reflect.TypeOf((*error)(nil)).Elem()

// Call
// a wrapper function that checks the returned values from a function call for
// error values. if the function returns an error that is non-nil we will set
// the returned boolean flag to true indicating something may have gone wrong
// inside the function call.
func (instance *runInstance) Call(function *pFunction, ins []reflect.Value) ([]reflect.Value, bool) {

	drop := false

	// append the values given to the function on-top of the injectables that need to
	// be passed to every step in the pipeline
	arguments := append(instance.injectables, ins...)
	results := function.Reflected.Value.Call(arguments)

	// TODO : the number of results returned by a function can be pre-computed
	numResults := len(results)

	// a common pattern in golang is to return (value, error) where error can be used
	// to identify that the function was not able to run to completion.
	//
	// the pipeline supports a similar method of identifying that the function failed
	// to run successfully to completion. when the function returns an error value
	// (that must be the last value returned) the pipeline will check whether the
	// error is nil or not to determine whether the resultant data should be used.
	//
	// error is nil -> keep sending the resultant data along the pipeline
	// error -> do NOT pass the data along the pipeline (aka. drop the data)
	if numResults > 0 {

		lastResult := results[numResults-1]

		isError := lastResult.Type().Implements(errorInterface)

		if isError {
			isNil := lastResult.IsNil()

			// the yule framework shall be responsible for displaying errors sent
			// by the pipeline to avoid requiring the developer to handle and return
			// the error which is considered an anti-pattern
			log.Println(lastResult.Elem())

			if !isNil {
				if instance.testing {
					instance.testingReport.Success = false
					instance.testingReport.Step = function.Identifier
					instance.testingReport.Cause = lastResult.Interface().(error)
				}
				drop = true
			}

			// never pass an error along the pipeline
			results = results[:numResults-1]
		}
	}

	return results, drop
}

func (instance *runInstance) Provision(function *pFunction) {
	instance.Event(StartProvision)
	defer instance.Event(EndProvision)

	// the runner must provision new threads one at a time.
	// note: avoid the possibility of >1 thread modifying the wait-group at a time
	instance.threadMutex.Lock()
	defer instance.threadMutex.Unlock()

	// a new function is provisioned
	// we should inform the wait group that the runner isn't finished until the wg is done
	instance.waitGroup.Add(1)

	go func(supervisor *runInstance, function *pFunction) {

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
					supervisor.waitGroup.Done()
				}
			}()

			// support passing values to the pipeline manually
			// todo : add more description
			if instance.testing {
				return
			}

			function.To.Value.AddProducer()

			if DEBUG {
				log.Printf("creating new producer function %d\n", supervisor.Id)
			}

			// TODO : reword
			// if the producer function has output, then don't worry about spawning a wrapper,
			// allow the function to return normally and send the data along the pipe

			if function.Reflected.Type.NumOut() > 0 {

				output := function.Reflected.Value.Call(instance.injectables)
				function.To.Value.Push(output)

			} else {

				select {
				case <-supervisor.ExtractWrapper(function, function.To.Value):
					break
				case <-supervisor.ExtractShutdownWrapper():
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

			quit := make(chan bool)
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
					supervisor.waitGroup.Done()
				}
			}()

			if DEBUG {
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
				case request := <-function.From.Value.GetChannel():
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
					supervisor.waitGroup.Done()
				}
			}()

			quit := make(chan bool)
			function.Mutex.Lock()
			function.Quit = append(function.Quit, quit)
			function.Mutex.Unlock()

			function.To.Value.AddProducer()

			if DEBUG {
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
				case request := <-function.From.Value.GetChannel():
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
							results, drop := supervisor.Call(function, request.Data)

							function.To.Mutex.Lock()
							if !drop {

								// PROPOSAL 6.
								// ~let there be two functions f1 and f2 that are transformers along the pipeline.
								// ~let f1 return a slice of type A s.t. []A is the passed along value
								// ~let f2 accept a value of type A s.t. we expect f1 to send us A
								//
								// if (f1 sends to f2) and (f1 returns []A while f2 accepts A) then:
								// the pipeline shall break apart the []A returned by f1
								// (and) push each element A from the slice to the successive function f2
								if len(results) > 0 && results[0].Kind() == reflect.Slice {

									// note: we should always have some receiver to pull data from the channel but
									//		 this is a safety guard if something goes wrong
									if len(function.To.Receiver) > 0 {
										nextFunction := function.To.Receiver[0]
										nextFunctionReflection := nextFunction.Reflected.Type

										// note: the value should accept some value or the pipeline we've provisioned
										// 		 is invalid and should have been rejected prior to this step
										if (nextFunctionReflection.NumIn() > 0) && (nextFunctionReflection.In(0).Kind() != reflect.Slice) {

											// f1 returns []A and f2 accepts A has been validated upto this point
											// we should break apart []A and send the data 1-by-1 to f2
											for i := 0; i < results[0].Len(); i++ {
												function.To.Stats.Pushed++
												function.To.Value.Push([]reflect.Value{results[0].Index(i)})
											}
										} else {
											// default functionality
											function.To.Stats.Pushed++
											function.To.Value.Push(results)
										}
									} else {
										// default functionality
										function.To.Stats.Pushed++
										function.To.Value.Push(results)
									}

								} else {
									// default functionality
									function.To.Stats.Pushed++
									function.To.Value.Push(results)
								}

							} else {
								function.From.Stats.Dropped++
							}
							function.To.Mutex.Unlock()
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
						results, drop := supervisor.Call(function, []reflect.Value{queuedRequests})

						function.To.Mutex.Lock()
						if !drop {
							function.To.Stats.Pushed++
							function.To.Value.Push(results)
						} else {
							function.From.Stats.Dropped++
						}
						function.To.Mutex.Unlock()
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
		supervisor.waitGroup.Done()
	}(instance, function)
}

func (instance *runInstance) Remove(function *pFunction) {

	if function.Stats.Active <= 0 {
		panic("attempting to quit when no functions are running")
	}

	quit := function.Quit[0]
	function.Quit = function.Quit[1:]
	quit <- true
}

func (instance *runInstance) send(data any, functionId ...string) {

	specifiedFunction := len(functionId) > 0

	// the developer needs to specify which root they want to mimic
	// when the pipeline's topology is more complex than a linear line
	if !specifiedFunction && (len(instance.Pipeline.Roots) > 1) {
		panic("testing non-linear pipelines requires specifying which generator function is being stubbed")
	}

	instance.startingWaitGroup.Wait()
	instance.testingReport.Success = true

	isDataSent := false
	for _, f := range instance.Pipeline.Roots {

		// do not push data to the pipeline when the function identifier is
		// specified and does not match.
		if specifiedFunction && (f.Identifier != functionId[0]) {
			continue
		}
		f.To.Value.AddProducer()
		f.To.Value.Push([]reflect.Value{reflect.ValueOf(data)})

		isDataSent = true
	}

	if !isDataSent {
		panic("attempt to test the pipeline failed, could not find a pipeline to push data to")
	}
}

func (instance *runInstance) close() {

	instance.startingWaitGroup.Wait()

	for _, f := range instance.Pipeline.Roots {
		f.Stats.Active--
		f.To.Value.ProducerDone()
		instance.waitGroup.Done()
	}
}

func (instance *runInstance) Deletable() bool {
	return (instance.State == Terminated) || (instance.State == Failed)
}

func (instance *runInstance) Print() {
	fmt.Printf("Id: %d\n", instance.Id)
}
