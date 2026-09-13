# ScalingFunctions Framework

<img src="./docs/assets/Pangolin_Icon.svg" width="50%">

`ScalingFunctions` is an open source framework for building vertically scalable data pipelines.

The language has powerfull `goroutine` and `channel` primatives that enable a developer to safely distribute compute on a multi-core system but delegates provisioning consumer goroutines on channels to the developer.

The framework aims to define a standard algorithm for provisioning consumer (goroutines) on a channel by monitoring arrival and consumption rates of data. Consumers increase when the consumption-rate lags arrival-rate.

The result is a "consious" pipeline that scales its functions based on demand when the algorithm is applied across multiple channels.

## Features

Features implemented on the latest version of the framework.

| Feature | Description |
| :- | :- |
| Channel Congestion | Channels monitor the arrival and drain rates to determine congestion. |
| Consumer Provisioning | Consumers are provisioned when the consumer's channel becomes congested.
| Consious Pipelines | A set of sequential consumers and channels that scale based on congestion. |
| Pipeline Statistics | Data tracking arrival, drain, and provisioning rates in a pipeline.

## Benchmarks

Benchmarks performed on the latest version of the framework.

| Version | Scenario | Total Data | Total Time |
|:--------| :- | :- | :- |
|  | | | |

> To be completed.

## Tutorials

### Getting Started in 5 Minutes
The fastest way to get started is to dive into some code. We walk through a three-function pipeline and
explain how the framework works [here](./docs/getting_started.md).

### Examples Using the Framework
Some examples that have been created to demonstrate how to use aspects of the framework [here](./docs/examples.md).

### Deep Dive into the Architecture
Skip over the basic introduction and understand how the framework breaks down pipeline creation and multithreads functions
[here](./docs/arhictecture.md).

## Disclaimer

The framework is made open-source under the GNU lesser general public licence. All source code made available has been developed without the use of AI generated code that may come from sources that violate the GNU lesser general public licence. The use of AI coding tools is welcomed without the use of generated code.  

> Contributions to the framework have been steady since 2022 closed source. I have decided to open-source the code in hopes that another developer out there finds value in it as I have.