"""Cube documentation for data modeling."""

CUBE_VIEWS = """
Views in Cube.js are a powerful way to create reusable combinations of cubes through joins. Unlike regular cubes that map directly to tables, views are virtual cubes built by joining other cubes.

Key aspects of views:
1. Views don't need sql or join definitions - they use the joins already defined in cubes
2. Views use includes/excludes to specify which fields to include from each cube
3. Views are ideal for creating denormalized representations of data spread across multiple cubes

Basic view structure:
```yaml
views:
  - name: orders_overview
    cubes:
      - join_path: base_orders
        includes:
          - status
          - created_date
          - total_amount
          - count
          - average_order_value
      
      - join_path: base_orders.users
        includes:
          - name
          - email
          - country
```

Example with advanced features:
```yaml
views:
  - name: detailed_orders
    cubes:
      - join_path: base_orders
        includes:
          - status
          - created_date
          - total_amount
      
      - join_path: base_orders.line_items.products
        includes:
          - name: name
            alias: product_name
          - category
          - price
      
      - join_path: base_orders.users
        prefix: true  # Will prefix all fields with 'users_'
        includes: "*"  # Include all fields
        excludes:  # Except these ones
          - password_hash
          - internal_id
```

Best practices for views:
1. Use views to create denormalized representations instead of complex joins within cube definitions
2. Use aliases to avoid naming conflicts between cubes
3. Use prefix: true when including many fields that might conflict
4. Use includes: "*" with excludes to include most fields except a few
5. Views inherit security contexts from their source cubes
"""

CUBE_SYNTAX = """
Cube.js uses YAML for data modeling. Each cube represents a table or view in your database.

Basic cube structure:
```yaml
cubes:
  Users:
    sql: SELECT * FROM users
    
    measures:
      count:
        type: count
    
    dimensions:
      age:
        sql: age
        type: number
      
      name:
        sql: CONCAT(first_name, ' ', last_name)
        type: string
```
"""

CUBE_JOINS = """
Joins are used to connect multiple cubes together. They can be defined in the joins section of a cube.

Example of common join types:
```yaml
cubes:
  - name: orders
    joins:
      - name: users
        relationship: many_to_one
        sql: "{CUBE.user_id} = {users.id}"
      
      - name: line_items
        relationship: one_to_many
        sql: "{CUBE.id} = {line_items.order_id}"

  - name: line_items
    joins:
      - name: products
        relationship: many_to_one
        sql: "{CUBE.product_id} = {products.id}"
```

Relationship types:
- many_to_one: Many records in this cube correspond to one record in the joined cube
- one_to_many: One record in this cube corresponds to many records in the joined cube
- one_to_one: One record in this cube corresponds to one record in the joined cube

Additional join properties:
- sql: The SQL condition for the join
- type: Optional join type (LEFT JOIN by default)
"""

CUBE_MEASURES = """
Measures are the quantitative data points in your cube - typically numeric values that can be aggregated.

Types of measures:
- count: Counts rows
- sum: Adds up values
- avg: Calculates average
- min/max: Find minimum/maximum values
- count_distinct: Counts unique values
- count_distinct_approx: Approximate count of unique values

Example:
```yaml
cubes:
  Orders:
    measures:
      revenue:
        type: sum
        sql: amount
        format: currency
      
      uniqueUsers:
        type: count_distinct
        sql: user_id
```
"""

CUBE_DIMENSIONS = """
Dimensions are the attributes by which you can slice and dice your measures.

Common dimension types:
- string: Text values
- number: Numeric values
- time: Date/time values
- boolean: True/false values

Example:
```yaml
cubes:
  Orders:
    dimensions:
      status:
        sql: status
        type: string
      
      createdAt:
        sql: created_at
        type: time
      
      isActive:
        sql: is_active
        type: boolean
```
"""

CUBE_OVERVIEW = """
Key concepts in Cube data modeling:
1. Cubes: Represent tables/views in your database
2. Measures: Numeric calculations or aggregations (e.g., count, sum)
3. Dimensions: Properties used to filter or group data
4. Joins: Connections between cubes
5. Pre-aggregations: Performance optimization through materialized views
"""

CUBE_REFERENCE = """
Cube properties:
- sql: The base SQL table or subquery
- sql_alias: Optional alias for the SQL table
- refresh_key: Defines the refresh strategy
- data_source: Specifies the data source
- description: Documentation for the cube
- shown: Controls cube visibility

Measure types:
- count
- sum
- avg
- min
- max
- count_distinct
- count_distinct_approx
- running_total
- number

Dimension types:
- string
- number
- boolean
- time
- geo

Join types:
- many_to_one
- one_to_many
- one_to_one

Don't use old join types:
- has_many
- belongs_to
- has_one
"""

CUBE_TYPES = """
Common measure types:
- count: Counts all records in the dataset
- sum: Adds up the values in a specific column
- avg: Calculates the average (mean) of values
- min: Finds the minimum value
- max: Finds the maximum value
- number: Returns a single numeric value
- count_distinct: Counts unique values
- count_distinct_approx: Approximate count of unique values (more efficient for large datasets)
- running_total: Calculates cumulative sum

Common dimension types:
- string: Text values
- number: Numeric values
- time: Timestamps and dates
- boolean: True/False values

Special properties:
- primary_key: Marks a dimension as the unique identifier for the cube
  Example:
  ```yaml
  dimensions:
    id:
      sql: id
      type: number
      primary_key: true
  ```

Format options for dimensions:
- image: URL to an image
- link: Clickable URL
- currency: Monetary values with currency symbol
- percent: Percentage values
- id: Unique identifiers
- datetime: {
    - year
    - quarter
    - month
    - week
    - date (YYYY-MM-DD)
    - hour
    - minute
    - second
  }

Examples:
```yaml
measures:
  revenue:
    sql: amount
    type: sum
  unique_users:
    sql: user_id
    type: count_distinct
  average_order_value:
    sql: amount
    type: avg

dimensions:
  id:
    sql: id
    type: number
    primary_key: true
  created_at:
    sql: created_at
    type: time
  status:
    sql: status
    type: string
  amount:
    sql: amount
    type: number
    format: currency
```
"""

CUBE_BEST_PRACTICES = """
Best practices for Cube modeling:
1. Use meaningful names for cubes, measures, and dimensions
2. Document your data model with descriptions
3. Use appropriate types and formats
4. Define relationships between cubes correctly
5. Consider performance implications of joins
6. Use pre-aggregations for large datasets
7. Follow naming conventions consistently
8. Group related measures and dimensions logically
9. Use security contexts where needed
10. Implement proper refresh strategies
11. It is normal for sections of a Cube definition to be joins then dimensions then measures
12. Don't output types in camelCase use snake_case instead
13. View fields selected in includes statement don't need types or sql as these are inherited from Cubes
14. Measures and dimensions cannot be defined in a View, only in a Cube
"""

def get_all_docs() -> str:
    """Get all Cube documentation as a single string."""
    return "\n\n".join(
        [
            "=== CUBE DOCUMENTATION ===",
            "--- Data Modeling Syntax ---",
            CUBE_SYNTAX,
            "--- Working with Views ---",
            CUBE_VIEWS,
            "--- Working with Joins ---",
            CUBE_JOINS,
            "--- Measures ---",
            CUBE_MEASURES,
            "--- Dimensions ---",
            CUBE_DIMENSIONS,
            "--- Overview ---",
            CUBE_OVERVIEW,
            "--- Reference Guide ---",
            CUBE_REFERENCE,
            "--- Types and Formats ---",
            CUBE_TYPES,
            "--- Best Practices ---",
            CUBE_BEST_PRACTICES,
        ]
    )
