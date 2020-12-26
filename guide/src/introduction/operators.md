# Operators

A streaming operator takes data and executes some form of transformation before
sending it downstream to other operators as part of a larger dataflow graph.

Arcon provides a set of pre-defined operators such as map, filter, and flatmap. However, it is also
possible to implement your own Operator.

An `Operator` has 4 associated types:

*   `IN` The type data flowing into the operator
*   `OUT` The output type of the executed transformation
*   `TimerState` If a timer is used, one may specify a type of timer state.
*   `OperatorState` Custom defined user state that is of type ArconState.

and 3 required methods to implement:

*   `handle_element` This method is called on each new streaming record that comes
*   `handle_timeout` This method handles custom triggered timers that have been scheduled through the `OperatorContext`
*   `handle_persist` This method defines how custom defined OperatorState shall be persisted.


If you do not care about dealing with timers or handling state, you may simply ignore these
methods by using the `ignore_timeout!()` and `ignore_persist()` macros that add empty implementations.

Down below is an example of a custom `Operator` that is stateless and performs a basic addition operation
on the incoming u64 data.

```rust,edition2018,no_run,noplaypen
{{#rustdoc_include ../../examples/src/bin/custom_operator.rs:operator}}
```
