# Add AUDIT_ONLY Model Kind for Multi-Table Validation

## Summary
This PR introduces a new `AUDIT_ONLY` model kind to SQLMesh, addressing the gap in validating relationships between multiple tables without materializing unnecessary tables. This feature combines the benefits of models (DAG participation, dependencies, scheduling) with audit behavior (validation without materialization).

## Problem Statement
Previously, SQLMesh users had to choose between:
- Creating wasteful materialized models just to run cross-table validations
- Using standalone audits that don't integrate well with model dependencies
- Building external validation systems outside SQLMesh

## Solution
The `AUDIT_ONLY` model kind enables users to:
- Validate relationships across multiple tables (e.g., referential integrity)
- Run complex validation queries that don't belong to a single model
- Participate in the model DAG with proper dependencies
- Avoid creating unnecessary materialized tables

## Implementation Details

### Core Changes

#### 1. Model Kind Definition (`sqlmesh/core/model/kind.py`)
- Added `AUDIT_ONLY` to `ModelKindName` enum
- Created `AuditOnlyKind` class with configuration:
  - `blocking` (default: `True`): Whether failures stop the pipeline
  - `max_failing_rows` (default: `10`): Number of sample rows in error messages
- Marked as `is_symbolic=True` (no materialization)

#### 2. Execution Strategy (`sqlmesh/core/snapshot/evaluator.py`)
- Created `AuditOnlyStrategy` extending `SymbolicStrategy`
- Executes validation query and checks for returned rows
- Raises `AuditError` with sample data if validation fails
- Properly integrated with the evaluation strategy routing

#### 3. Parser Support (`sqlmesh/core/dialect.py`)
- Added `AUDIT_ONLY` to list of model kinds that accept properties

#### 4. Snapshot Definition (`sqlmesh/core/snapshot/definition.py`)
- Fixed `evaluatable` property to include audit-only models
- Ensures proper interval tracking for validation execution

### Testing

#### Unit Tests (`tests/core/test_model.py`)
- 6 unit tests covering:
  - Basic parsing and properties
  - Blocking/non-blocking configuration
  - Max failing rows configuration
  - Python model support
  - Full configuration scenarios
  - Serialization/deserialization

#### Integration Tests (`tests/core/test_integration.py`)
- 6 integration tests validating:
  - Validation success/failure scenarios
  - Blocking vs non-blocking behavior
  - Dependency tracking
  - Scheduling with cron
  - Metadata changes

### Documentation

#### User Documentation Updates
- **`docs/concepts/audits.md`**: Added comprehensive AUDIT_ONLY section under Advanced Usage
- **`docs/concepts/models/model_kinds.md`**: Added detailed AUDIT_ONLY section with examples
- **`docs/reference/model_configuration.md`**: Added AUDIT_ONLY configuration reference

#### Example Models (`examples/sushi/models/`)
Added 3 demonstration models (all non-blocking for demo purposes):
- `audit_order_integrity.sql`: Validates referential integrity
- `audit_waiter_revenue_anomalies.sql`: Detects revenue anomalies
- `audit_duplicate_orders.sql`: Identifies duplicate orders

## Usage Example

```sql
MODEL (
    name data_quality.order_validation,
    kind AUDIT_ONLY (
        blocking TRUE,
        max_failing_rows 20
    ),
    depends_on [orders, customers],
    cron '@daily'
);

-- Query returns 0 rows for success
SELECT 
    o.order_id,
    o.customer_id,
    'Missing customer record' as issue
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;
```

## Key Differences from Traditional Audits

| Feature | Traditional Audits | AUDIT_ONLY Models |
|---------|-------------------|-------------------|
| **Scope** | Single model | Multiple models |
| **Dependencies** | Implicit | Explicit via depends_on |
| **Materialization** | N/A | Never materializes |
| **Location** | `audits/` directory | `models/` directory |
| **Scheduling** | With parent model | Independent cron |
| **DAG Participation** | Attached to model | Full model in DAG |

## Migration Path
- No breaking changes to existing models or audits
- Optional feature - only use when needed
- Can gradually migrate complex audits to audit-only models

## Testing Instructions

1. **Run unit tests:**
   ```bash
   pytest tests/core/test_model.py -k audit_only -xvs
   ```

2. **Run integration tests:**
   ```bash
   pytest tests/core/test_integration.py -k audit_only -xvs
   ```

3. **Try the sushi examples:**
   ```bash
   cd examples/sushi
   sqlmesh plan
   # Note: Example models are non-blocking so they won't fail the pipeline
   ```

4. **Create a test AUDIT_ONLY model:**
   ```sql
   -- Save as models/test_audit.sql
   MODEL (
       name test.audit_validation,
       kind AUDIT_ONLY,
       depends_on [your_table1, your_table2]
   );
   
   -- This should return 0 rows for success
   SELECT * FROM your_table1 
   WHERE some_condition_that_indicates_invalid_data;
   ```

## Checklist
- [x] Add `AUDIT_ONLY` to `ModelKindName` enum
- [x] Create `AuditOnlyKind` class
- [x] Update `ModelKind` Union type
- [x] Update `MODEL_KIND_NAME_TO_TYPE` mapping
- [x] Create `AuditOnlyStrategy` class
- [x] Update `_evaluation_strategy` routing
- [x] Add `is_audit_only` properties
- [x] Write unit tests
- [x] Write integration tests
- [x] Update documentation
- [x] Add examples to sushi demo project

## Related Issues
Addresses the need for multi-table validation without materialization as described in the RFC.

## Notes for Reviewers
- The feature is designed to be non-intrusive and backward compatible
- Example models in sushi are set to non-blocking to avoid disrupting tests
- Documentation emphasizes when to use AUDIT_ONLY vs traditional audits
- The implementation follows existing SQLMesh patterns for symbolic models

## Future Enhancements (Not in this PR)
- Support for incremental validation by time range
- Configurable number of failing rows to capture
- Warning mode that logs issues without failing
- Different visualization in UI/lineage graph