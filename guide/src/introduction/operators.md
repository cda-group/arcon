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
methods by using the `ignore_timeout!()` and `ignore_persist!()` macros that add empty implementations.

Both `handle_element` and `handle_timeout` has access to an `OperatorContext`. From this context, you may
output events, schedule timers, and also perform some logging.

## Creating an Operator

To showcase the Operator interface, we will now create two custom Operators. One that is very basic and
another that uses the timer facilities.

Down below we have created an Operator called `MyOperator`, it receives u64s and outputs a CustomEvent.

```rust,edition2018,no_run,noplaypen
{{#rustdoc_include ../../examples/src/bin/custom_operator.rs:operator}}
```

For context, this is how the CustomEvent struct looks like.

```rust,edition2018,no_run,noplaypen
{{#rustdoc_include ../../examples/src/bin/custom_operator.rs:data}}
```

We then create another Operator called `TimerOperator` that receives CustomEvent's and
schedules a timer event of u64 in the future when the event time reaches `current_time + 1000`.
The timer event is scheduled based on the key of the ArconType. Only one timer event can exist 
per key.

```rust,edition2018,no_run,noplaypen
{{#rustdoc_include ../../examples/src/bin/custom_operator.rs:timer_operator}}
```

Finally, we create a simple Pipeline to that uses our custom operators.


```rust,edition2018,no_run,noplaypen
{{#rustdoc_include ../../examples/src/bin/custom_operator.rs:pipeline}}
```
