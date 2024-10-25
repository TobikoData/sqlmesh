# Signals guide

SQLMesh's [built-in scheduler](./scheduling.md#built-in-scheduler) controls which models are evaluated when the `sqlmesh run` command is executed.

It determines whether to evaluate a model based on whether the model's [`cron`](../concepts/models/overview.md#cron) has elapsed since the previous evaluation. For example, if a model's `cron` was `@daily`, the scheduler would evaluate the model if its last evaluation occurred on any day before today.

Unfortunately, the world does not always accommodate our data system's schedules. Data may land in our system _after_ downstream daily models already ran. The scheduler did its job correctly, but today's late data will not be processed until tomorrow's scheduled run.

You can use signals to prevent this problem.

## What is a signal?

The scheduler uses two criteria to determine whether a model should be evaluated: whether its `cron` elapsed since the last evaluation and whether it upstream dependencies' runs have completed.

Signals allow you to specify additional criteria that must be met before the scheduler evaluates the model.

A signal definition has two components: a "checking" function that checks whether a criterion is met and a "factory function" that provides the checking function to SQLMesh. Before describing the checking function, we provide some background information about how the scheduler works.

The scheduler doesn't actually evaluate "a model" - it evaluates a model over a specific time interval. This is clearest for incremental models, where only rows in the time interval are ingested during an evaluation. However, evaluation of non-temporal model kinds like `FULL` and `VIEW` are also based on a time interval: the model's `cron` frequency.

The scheduler's decisions are based on these time intervals. For each model, the scheduler examines a set of candidate intervals and identifies the ones that are ready for evaluation.

It then divides those into _batches_ (configured with the model's [batch_size](../concepts/models/overview.md#batch_size) parameter). For incremental models, it evaluates the model once for each batch. For non-incremental models, it evaluates the model once if any batch contains an interval.

Signal checking functions examines a batch of time intervals. The function has two inputs: signal metadata values defined your model and a batch of time intervals. It may return `True` if all intervals are ready for evaluation, `False` if no intervals are ready, or the time intervals themselves if only some are ready. A checking function is defined as a method on a `Signal` sub-class.

A project may have one signal factory function. The factory function determines which checking function should be used for a given model and signal. Its inputs are the signal metadata values defined in your model, and it returns the checking function.

## Defining a signal

To define a signal, create a `signals` directory in your project folder. Define your signal in a file named `__init__.py` in that directory.

The file must:

- Define at least one `Signal` sub-class containing a `check_intervals` method
- Define a factory function that returns a `Signal` sub-class and decorate the function with the `@signal_factory` decorator

We now demonstrate signals of varying complexity.

### Simple example

This example defines the `RandomSignal` class and its mandatory `check_intervals()` method.

The method returns `True` (indicating that all intervals are ready for evaluation) if a random number is greater than a threshold specified in the model definition:

```python linenums="1"
import random
import typing as t
from sqlmesh.core.scheduler import signal_factory, Batch, Signal

class RandomSignal(Signal):
    def __init__(
        self,
        signal_metadata: t.Dict[str, t.Union[str, int, float, bool]]
    ):
        self.signal_metadata = signal_metadata

    def check_intervals(self, batch: Batch) -> t.Union[bool, Batch]:
        threshold = self.signal_metadata["threshold"]
        return random.random() > threshold
```

Note that the `RandomSignal` class sub-classes `Signal` and takes a `signal_metadata` argument.

The `check_intervals()` method extracts the threshold metadata and compares a random number to it.

We can now add a factory function that returns the `RandomSignal` sub-class. Note the `@signal_factory` decorator on line 8:

```python linenums="1" hl_lines="8-10"
import random
import typing as t
from sqlmesh.core.scheduler import signal_factory, Batch, Signal

class RandomSignal(Signal):
    def __init__(
        self,
        signal_metadata: t.Dict[str, t.Union[str, int, float, bool]]
    ):
        self.signal_metadata = signal_metadata

    def check_intervals(self, batch: Batch) -> t.Union[bool, Batch]:
        threshold = self.signal_metadata["threshold"]
        return random.random() > threshold

@signal_factory
def my_signal_factory(signal_metadata: t.Dict[str, t.Union[str, int, float, bool]]) -> Signal:
    return RandomSignal(signal_metadata)
```

We now have a working signal!

We specify that a model should use the signal by passing metadata to the model DDL's `signals` key.

The `signals` key accepts an array delimited by brackets `[]`. Each tuple in the list should contain the metadata needed for one signal evaluation.

This example specifies that the `RandomSignal` should evaluate once with a threshold of 0.5:

```sql linenums="1" hl_lines="4-6"
MODEL (
  name example.signal_model,
  kind FULL,
  signals [
    (threshold = 0.5), # specify threshold value
  ]
);

SELECT 1
```

The next time this project is `sqlmesh run`, our signal will metaphorically flip a coin to determine whether the model should be evaluated.

### Advanced Example

This example demonstrates more advanced use of signals, including:

- Multiple signals in one model
- A signal returning a subset of intervals from a batch (rather than a single `True`/`False` value for all intervals in the batch)

In this example, there are two signals.

```python
import typing as t
from datetime import datetime

from sqlmesh.core.scheduler import signal_factory, Batch, Signal
from sqlmesh.utils.date import to_datetime


class AlwaysReady(Signal):
    # signal that indicates every interval is always ready
    def check_intervals(self, batch: Batch) -> t.Union[bool, Batch]:
        return True


class OneweekAgo(Signal):
    def __init__(self, dt: datetime):
        self.dt = dt

    # signal that returns only intervals that are <= 1 week ago
    def check_intervals(self, batch: Batch) -> t.Union[bool, Batch]:
        return [
            (start, end)
            for start, end in batch
            if start <= self.dt
        ]
```
Instead of returning a single `True`/`False` value for whether a batch of intervals is ready for evaluation, the `OneweekAgo` signal class returns specific intervals from the batch.

Its `check_intervals` method accepts a `dt` datetime argument, to which It compares the beginning of each interval in the batch. If the interval start is before that argument, the interval is ready for evaluation and included in the returned list.
These signals can be added to a model like so. Now that we have more than one signal, we must have a way to tell the signal factory which signal should be called.

In this example, we use the `kind` key to tell the signal factory which signal class should be called. The key name is arbitrary, and you may choose any key name you want.

This example model specifies two `kind` values so both signals are called.

```sql linenums="1" hl_lines="7-10"
MODEL (
  name example.signal_model,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column ds,
  ),
  start '2 week ago',
  signals [
    (kind = 'a'),
    (kind = 'b'),
  ]
);


SELECT @start_ds AS ds
```

Our signal factory definition extracts the `kind` key value from the `signal_metadata`, then instantiates the signal class corresponding to the value:

```python linenums="1" hl_lines="3-3"
@signal_factory
def my_signal_factory(signal_metadata: t.Dict[str, t.Union[str, int, float, bool]]) -> Signal:
    kind = signal_metadata["kind"]

    if kind == "a":
        return AlwaysReady()
    if kind == "b":
        return OneweekAgo(to_datetime("1 week ago"))
    raise Exception(f"Unknown signal {kind}")
```

