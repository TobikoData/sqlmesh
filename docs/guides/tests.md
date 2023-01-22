# Testing
Tests are one of the tools SQLMesh provides to validate your models. Along with [audits](/guides/audits), they are a great way to ensure the quality of your data and to build trust in it across your organization. 

Similar to unit tests in software engineering, SQLMesh tests compare inputs and the expected outputs with the output from your model query; you can even test individual CTEs in your model queries. These tests are input and output data fixtures defined in YAML files in a test directory in your project. A comprehensive suite of tests can empower your data engineers and analysts to work with confidence, since you can validate that downstream models are behaving as expcted when model changes are made.

## Example test
In the following example, we define a model test for the `sushi.customer_revenue_by_day` model to ensure the model query behaves as expcted. The test provides upstream data as input for the model, as well as expected output for the model and a CTE used by the model. SQLMesh will load the input rows, execute your model's CTE and query, and compare them to the output rows:

```yaml
test_customer_revenue_by_day:
  model: sushi.customer_revenue_by_day
  inputs:
    sushi.orders:
      rows:
        - id: 1
          customer_id: 1
          waiter_id: 1
          start_ts: 2022-01-01 01:59:00
          end_ts: 2022-01-01 02:29:00
          ds: 2022-01-01
        - id: 2
          customer_id: 1
          waiter_id: 2
          start_ts: 2022-01-01 03:59:00
          end_ts: 2022-01-01 03:29:00
          ds: 2022-01-01
    sushi.order_items:
      rows:
        - id: 1
          order_id: 1
          item_id: 1
          quantity: 2
          ds: 2022-01-01
        - id: 2
          order_id: 1
          item_id: 2
          quantity: 3
          ds: 2022-01-01
        - id: 3
          order_id: 2
          item_id: 1
          quantity: 4
          ds: 2022-01-01
        - id: 4
          order_id: 2
          item_id: 2
          quantity: 5
          ds: 2022-01-01
    sushi.items:
      rows:
        - id: 1
          name: maguro
          price: 1.23
          ds: 2022-01-01
        - id: 2
          name: ika
          price: 2.34
          ds: 2022-01-01
  outputs:
    vars:
      start: 2022-01-01
      end: 2022-01-01
      latest: 2022-01-01
    ctes:
      order_total:
        rows:
        - order_id: 1
          total: 9.48
          ds: 2022-01-01
        - order_id: 2
          total: 16.62
          ds: 2022-01-01
    query:
      rows:
      - customer_id: 1
        revenue: 26.1
        ds: '2022-01-01'
```

## Run a test
### The CLI test command
You can execute tests with the `sqlmesh test` command as follows:
```
% sqlmesh --path examples/sushi test
...F
======================================================================
FAIL: test_customer_revenue_by_day (examples/sushi/models/tests/test_customer_revenue_by_day.yaml:1)
----------------------------------------------------------------------
AssertionError: Data differs
- {'customer_id': 1, 'revenue': 26.2, 'ds': '2022-01-01'}
?                                  ^

+ {'customer_id': 1, 'revenue': 26.1, 'ds': '2022-01-01'}
?                                  ^


----------------------------------------------------------------------
Ran 4 tests in 0.030s

FAILED (failures=1)
```

As the tests run, SQLMesh will identify any that fail.

To run a specific model test, pass in the module followed by `::` and the name of the test, such as
`project.tests.test_order_items::test_single_order_item`. 

You can also run tests that match a pattern or substring using a glob pathname expansion syntax. For example, `project.tests.test_order*` will match `project.tests.test_orders` and `project.tests.test_order_items`.

## Advanced usage
### Reusable Data Fixtures
