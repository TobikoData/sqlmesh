# Signals guide

SQLMesh's [built-in scheduler](./scheduling.md#built-in-scheduler) controls which models are evaluated when the `sqlmesh run` command is executed.

It determines whether to evaluate a model based on whether the model's [`cron`](../concepts/models/overview.md#cron) has elapsed since the previous evaluation. For example, if a model's `cron` was `@daily`, the scheduler would evaluate the model if its last evaluation occurred on any day before today.

Unfortunately, the world does not always accommodate our data system's schedules. Data may land in our system _after_ downstream daily models already ran. The scheduler did its job correctly, but today's late data will not be processed until tomorrow's scheduled run.

You can use signals to prevent this problem.

## What is a signal?

The scheduler uses two criteria to determine whether a model should be evaluated: whether its `cron` elapsed since the last evaluation and whether it upstream dependencies' runs have completed.

Signals allow you to specify additional criteria that must be met before the scheduler evaluates the model.

A signal definition is simply a function that checks whether a criterion is met. Before describing the checking function, we provide some background information about how the scheduler works.

The scheduler doesn't actually evaluate "a model" - it evaluates a model over a specific time interval. This is clearest for incremental models, where only rows in the time interval are ingested during an evaluation. However, evaluation of non-temporal model kinds like `FULL` and `VIEW` are also based on a time interval: the model's `cron` frequency.

The scheduler's decisions are based on these time intervals. For each model, the scheduler examines a set of candidate intervals and identifies the ones that are ready for evaluation.

It then divides those into _batches_ (configured with the model's [batch_size](../concepts/models/overview.md#batch_size) parameter). For incremental models, it evaluates the model once for each batch. For non-incremental models, it evaluates the model once if any batch contains an interval.

Signal checking functions examines a batch of time intervals. The function is always called with a batch of time intervals (DateTimeRanges). It can also optionally be called with key word arguments. It may return `True` if all intervals are ready for evaluation, `False` if no intervals are ready, or the time intervals themselves if only some are ready. A checking function is defined with the `@signal` decorator.

## Defining a signal

To define a signal, create a `signals` directory in your project folder. Define your signal in a file named `__init__.py` in that directory (you can have additional python file names as well).

A signal is a function that accepts a batch (DateTimeRanges: t.List[t.Tuple[datetime, datetime]]) and returns a batch or a boolean. It needs use the @signal decorator.

We now demonstrate signals of varying complexity.

### Simple example

This example defines a `RandomSignal` method.

The method returns `True` (indicating that all intervals are ready for evaluation) if a random number is greater than a threshold specified in the model definition:

```python linenums="1"
import random
import typing as t
from sqlmesh import signal, DatetimeRanges


@signal()
def random_signal(batch: DatetimeRanges, threshold: float) -> t.Union[bool, DatetimeRanges]:
    return random.random() > threshold
```

Note that the `random_signal()` takes a mandatory user defined `threshold` argument.

The `random_signal()` method extracts the threshold metadata and compares a random number to it. The type is inferred based on the same [rules as SQLMesh Macros](../concepts/macros/sqlmesh_macros.md#typed-macros).

Now that we have a working signal, we need to specify that a model should use the signal by passing metadata to the model DDL's `signals` key.

The `signals` key accepts an array delimited by brackets `[]`. Each function in the list should contain the metadata needed for one signal evaluation.

This example specifies that the `random_signal()` should evaluate once with a threshold of 0.5:

```sql linenums="1" hl_lines="4-6"
MODEL (
  name example.signal_model,
  kind FULL,
  signals [
    random_signal(threshold := 0.5), # specify threshold value
  ]
);

SELECT 1
```

The next time this project is `sqlmesh run`, our signal will metaphorically flip a coin to determine whether the model should be evaluated.

### Advanced Example

This example demonstrates more advanced use of signals: a signal returning a subset of intervals from a batch (rather than a single `True`/`False` value for all intervals in the batch)

```python
import typing as t

from sqlmesh import signal, DatetimeRanges
from sqlmesh.utils.date import to_datetime


# signal that returns only intervals that are <= 1 week ago
@signal()
def one_week_ago(batch: DatetimeRanges) -> t.Union[bool, DatetimeRanges]:
    dt = to_datetime("1 week ago")

    return [
        (start, end)
        for start, end in batch
        if start <= dt
    ]
```

Instead of returning a single `True`/`False` value for whether a batch of intervals is ready for evaluation, the `one_week_ago()` function returns specific intervals from the batch.

It generates a datetime argument, to which it compares the beginning of each interval in the batch. If the interval start is before that argument, the interval is ready for evaluation and included in the returned list.
These signals can be added to a model like so.

```sql linenums="1" hl_lines="7-10"
MODEL (
  name example.signal_model,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column ds,
  ),
  start '2 week ago',
  signals [
    one_week_ago(),
  ]
);


SELECT @start_ds AS ds
```
