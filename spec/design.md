# Design document for durable workflow library

The goal is to create a Scala 3 library for durable workflows. A durable workflow is characterized by:
- being able to resume execution after a crash (state persisted, even if the process is killed)
- steps with side effects have guarantees about how many times they are executed (at-most-once, at-least-once)
- workflows may be long-running (hours, days, weeks) and wait for external events (signals, timers, other workflows) to resume execution

## Primary design goals
- simple and intuitive API for defining workflows, without all the boilerplate of defining a state machine like in Workflow4s
- pluggable backends:  in-memory, Postgres, Kafka, Redis etc. Only Postgres and in-memory will be implemented for now
- API is in Scala 3 direct style, not monadic like cats-effect, but is still functional programming oriented, uses type-safety and Scala 3 contextual abstractions
- maintainable workflow definitions over time: There must be a way to evolve workflow definitions without losing data or breaking workflows in-progress 
- manual intervention: There must be a way to manually restart workflows at specific steps, or to manually set signals, in case of bugs

## Workflow definitions

A workflow is defined as a regular function + some metadata (workflow id, workflow description). The workflow function may take one or multiple input parameters. The workflow function consist of regular code, where side-effecting operations that must be executed in a durable way (at-least-once, at-most-once) are wrapped in a `Step` construct (lambda function). Example:

```scala
val workflow = Workflow("my-workflow", "This is my workflow") { (input1: String, input2: Int) =>
  val step1Result = Step("step-1", "This is step 1") {
    // some side-effecting operation that must be executed at-least-once
    doSomething(input1)
  }

  val step2Result = Step("step-2", "This is step 2") {
    // some side-effecting operation that must be executed at-most-once
    doSomethingElse(step1Result, input2)
  }

  // return the final result of the workflow
  step2Result
}
```
This API is just a draft, and will be refined as we go along.

## Running workflows

- create workflow and start it immediately
- create workflow and not start it immediately (in which use-cases is this necessary?)
  - Do we need a class that is some kind of "handle" to a workflow instance? A workflow instance is identified by its workflow id + instance key. So how would this handle be different from WorkflowContext? The handle can hardly "be" the workflow instance, in execution, because the workflow instance in execution is just a thread that is executing the workflow function.
- create workflow idempotently
  - throw error if input parameters are different from first creation
- wait blockingly for a workflow to complete and get its result -> Function needs to return WorkflowStopped when workflow waits on signal/timer.

- start all workflows matching a key prefix
- abandon all workflows matching a key prefix


# Workflow Steps

- executing steps in parallel
- executing the same steps for each element in a collection
  - in parallel
  - sequential
  - racing (first to complete)
  - Do we have to implement this as a special feature, or can we somehow have a general mechanism? See which concurrency patterns are provided by cats-effect and ox for this.
  - How do we prevent O(n^2) executions when each element's subworkflow waits for a signal: Each time one element's subworkflow is resumed, all other subworkflows will be resumed as well, and they will all wait for the signal again.
- steps with cache validity (TTL)
- steps that invalidate their cache when inputs change
  - either set cache-key manually or use macro to find all input variables automatically?
- versioning of steps (in which use-cases is this necessary?)
- at-least-once steps vs at-most-once steps

# Signals and Timers

- workflow waiting on a timer to resume execution
- workflow waiting on a signal to resume execution
- workflow waiting on another workflow to complete before resuming execution
- workflow combining multiple signals:
  - racing signals
  - waiting for all signals to be set (and combining the values of all of them)
  - signals as streams: 
    - combine latest values of multiple signals, and resume when any of them changes
    - combine latest value of one signal with all values from other signal that have been set since the last time the first signal was set
    - Do we have to implement this as a special feature, or can we somehow have a general mechanism for combining signals that can be used to implement this?
- send signal to multiple workflows at once (by key prefix)

