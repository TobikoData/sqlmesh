# Note: `sqlmesh_dbt` is deliberately in its own package from `sqlmesh` to avoid the upfront time overhead
# that comes from `import sqlmesh`
#
# Obviously we still have to `import sqlmesh` at some point but this allows us to defer it until needed,
# which means we can make the CLI feel more responsive by being able to output something immediately
