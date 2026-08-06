# Getting Started in 5 Minutes

ScalingFunctions is a powerful framework that allows you to build multithreaded pipelines in a couple lines of code. The pipelines
can be as simple as an [ETL]() set of functions or a series of sequential functions processing data.

Our first example is a three function pipelines; a generator function that creates data, a transform function that 
multiplies the generated data, and a print function that outputs the transformed data.

## Setting up our Environment

Start by created a new folder for your project and initializing a go module.

```shell
mkdir myproject
cd myproject/
go mod init
```

Install the framework so that we can get to work right away.

```shell
go install git@github.com:GabeCordo/ScalingFunctions.git
```

## Writting Our Functions

Create a *main.go* file where we will define our code. Let's create the functions that
are used in the pipeline.

![](.bin/basic_example.png)

### Generator Function
The generator function accepts a single parameter, a pipe that it can push data to. The function
will create 10 sequentially increasing integers that are pushed to the pipeline.

```go
func gen(out chan int) {

	defer close(out)

	for i := 0; i < 10; i++ {
		out <- i
	}
}
```

### Transformer Function
The transformer function accepts a single parameter, an integer which it will multiply
by a scalar two. The function returns a single value, the transformed integer.

```go
func mul(c int) (d int) {
	d = c * 2
	return d
}
```

### Print Function
The print function accepts a single parameter, an integer which it will print to the standard output.

```go
func prt(d int) {
	fmt.Println(d)
}
```

## Creating the Pipeline

Pipelines are created in a series of steps that remain the same no matter what you are creating. A pipeline from start till end can be
considered a *sequential **branch** of executable functions*. 

### Branches 
**Branches** are used to define a series of functions that will be executed one after another. You define a new branch with the
*NewBranch()* keyword and defines function order using a builder pattern approach. The *Add()* keyword is used to define
the next function in the branch which accepts a single argument, the function.

Let's create a new branch which chains our three functions together. The order of execution
is always from the left to right.
```go
b := NewBranch().Add(gen).Add(mul).Add(prt)
```

### Metadata 
The branch is only a high level description of a series of functions. It's possible we may want multiple branches of execution to interact to
form a more complex pipeline that has a tree rather than linear structure. We won't go into this now but keep in mind this next
step is used to facilitate more complex pipelines.

A detailed description of the pipeline is contained in the **Metadata** structure. The metadata describes the functions,
pre-computed types, and pipes used to send data between functions in the pipeline. The metadata structure can describe tree structures unlike
the Branch structure.

Let's call the *Build()* function which takes the branch *b* we created as an argument. The function builds our branch into
a standard metadata format that can be used. 
```go
m := Build(b)
```

### Runnable

The metadata describes the functions, pipes, and types in a pipeline similar to a contract. The running instance of the
contract is known as the (pipeline) **Runnable**.

Let's call the *NewRunnable()* function which takes the metadata *m* we created before. The *Runnable* struct contains a 
blocking function *Run()* that executes the pipeline. Execution will return to the callee once the pipeline has completed.
```go
r := NewRunnable(bld)
r.Run()
```

## Let's look at what we have.

Our *main.go* file should look something like this. I have taken the liberty to join
the last three statements into a single line.

```go
func gen(out chan int) {

	defer close(out)

	for i := 0; i < 10; i++ {
		out <- i
	}
}

func mul(c int) (d int) {
    d = c * 2
    return d
}

func prt(d int) {
    fmt.Println(d)
}

func main() {

    b := NewBranch().Add(gen).Add(mul).Add(prt)
	NewRunnable(Build(b)).Run()
}
```

Running the code will result in the following output.

```shell
0
2
4
6
8
10
12
14
16
18
```