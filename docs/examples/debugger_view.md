# Debugger View

This view is used to help you debug production run issues with your SQLMesh models in Tobiko Cloud. Fixing data pipelines in production is a stressful, time-consuming process, so we're here to make it easier with a few clicks.

Here are some common issues you might run into and how to debug them:

- Schema evolution error
- Middle of DAG error

## How it works

When you run a plan or apply, we automatically run an audit of your model. If there are any issues, we'll show you the error in the debugger view.

## Schema Evolution Error

TODO: add screenshot of debugger view for schema evolution error

Let's say you have a model that looks like this:

```sql
select * from my_model
```

It's selecting from raw, upstream data outside of SQLMesh. Let's say someone updated the upstream data to modify a column name. 


## Middle of DAG Error

TODO: add screenshot of debugger view for middle of DAG error

Let's say you have a data pipeline that looks like this:

```
my_model -> my_other_model -> my_final_model
```

With code that looks like this:

```sql
select * from my_model
```

And let's say you run into an issue where the `my_other_model` is failing an audit. This is how you can debug it: