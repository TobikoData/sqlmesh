# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Agent-Based Development Workflow

Every time the user requests a feature or bug fix, you MUST follow the process below:

### Development Process

1. **Understanding The Task**: Use the `developer` agent to understand what the user is asking for and to read GitHub issues
2. **Feature Development & Bug Fixes**: Use the `developer` agent for implementing features and fixing bugs. IMPORTANT: Always begin by writing a failing test (or tests) that reflects the expected behavior
3. **Code Review**: After development work, invoke the `code-reviewer` agent to review the implementation
4. **Iteration**: Use the `developer` agent again to address feedback from the code reviewer
5. **Repeat**: Continue the developer → code-reviewer cycle until no more feedback remains
6. **Documentation**: If the feature or bug fix requires documentation updates, invoke the `technical-writer` agent

IMPORTANT: Make sure to share the project overview, architecture overview, and other concepts outlined below with the agent when it is invoked.

### Agent Responsibilities

**Developer Agent**:
- Understands a feature request or a reported issue
- Implements new features following SQLMesh's architecture patterns
- Fixes bugs with proper understanding of the codebase
- Writes comprehensive tests following SQLMesh's testing conventions
- Follows established code style and conventions

**Code-Reviewer Agent**:
- Reviews implementation for quality and architectural compliance
- Identifies potential issues, edge cases, and improvements
- Ensures adherence to SQLMesh patterns and best practices
- Validates test coverage and quality

**Technical-Writer Agent**:
- Creates and updates user-facing documentation
- Writes API documentation for new features
- Updates existing docs after code changes
- Creates migration guides and deep-dive technical explanations

## Project Overview

SQLMesh is a next-generation data transformation framework that enables:
- Virtual data environments for isolated development without warehouse costs
- Plan/apply workflow (like Terraform) for safe deployments
- Multi-dialect SQL support with automatic transpilation
- Incremental processing to run only necessary transformations
- Built-in testing and CI/CD integration

**Requirements**: Python >= 3.9 (Note: Python 3.13+ is not yet supported)

## Essential Commands

### Environment setup
```bash
# Create and activate a Python virtual environment (Python >= 3.9, < 3.13)
python -m venv .venv
source ./.venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install development dependencies
make install-dev

# Setup pre-commit hooks (important for code quality)
make install-pre-commit
```

### Common Development Tasks
```bash
# Run linters and formatters (ALWAYS run before committing)
make style

# Fast tests for quick feedback during development
make fast-test

# Slow tests for comprehensive coverage
make slow-test

# Run specific test file
pytest tests/core/test_context.py -v

# Run tests with specific marker
pytest -m "not slow and not docker" -v

# Build package
make package

# Serve documentation locally
make docs-serve
```

### Engine-Specific Testing
```bash
# DuckDB (default, no setup required)
make duckdb-test

# Other engines require credentials/Docker
make snowflake-test  # Needs SNOWFLAKE_* env vars
make bigquery-test   # Needs GOOGLE_APPLICATION_CREDENTIALS
make databricks-test # Needs DATABRICKS_* env vars
```

### UI Development
```bash
# In web/client directory
pnpm run dev   # Start development server
pnpm run build # Production build
pnpm run test  # Run tests

# Docker-based UI
make ui-up     # Start UI in Docker
make ui-down   # Stop UI
```

## Architecture Overview

### Core Components

**sqlmesh/core/context.py**: The main Context class orchestrates all SQLMesh operations. This is the entry point for understanding how models are loaded, plans are created, and executions happen.

**sqlmesh/core/model/**: Model definitions and kinds (FULL, INCREMENTAL_BY_TIME_RANGE, SCD_TYPE_2, etc.). Each model kind has specific behaviors for how data is processed.

**sqlmesh/core/snapshot/**: The versioning system. Snapshots are immutable versions of models identified by fingerprints. Understanding snapshots is crucial for how SQLMesh tracks changes.

**sqlmesh/core/plan/**: Plan building and evaluation logic. Plans determine what changes need to be applied and in what order.

**sqlmesh/core/engine_adapter/**: Database engine adapters provide a unified interface across 16+ SQL engines. Each adapter handles engine-specific SQL generation and execution.

### Key Concepts

1. **Virtual Environments**: Lightweight branches that share unchanged data between environments, reducing storage costs and deployment time.

2. **Fingerprinting**: Models are versioned using content-based fingerprints. Any change to a model's logic creates a new version.

3. **State Sync**: Manages metadata across different backends (can be stored in the data warehouse or external databases).

4. **Intervals**: Time-based partitioning system for incremental models, tracking what data has been processed.

## Important Files

- `sqlmesh/core/context.py`: Main orchestration class
- `examples/sushi/`: Reference implementation used in tests
- `web/server/main.py`: Web UI backend entry point
- `web/client/src/App.tsx`: Web UI frontend entry point
- `vscode/extension/src/extension.ts`: VSCode extension entry point

## GitHub CI/CD Bot Architecture

SQLMesh includes a GitHub CI/CD bot integration that automates data transformation workflows. The implementation is located in `sqlmesh/integrations/github/` and follows a clean architectural pattern.

### Code Organization

**Core Integration Files:**
- `sqlmesh/cicd/bot.py`: Main CLI entry point (`sqlmesh_cicd` command)
- `sqlmesh/integrations/github/cicd/controller.py`: Core bot orchestration logic
- `sqlmesh/integrations/github/cicd/command.py`: Individual command implementations
- `sqlmesh/integrations/github/cicd/config.py`: Configuration classes and validation

### Architecture Pattern

The bot follows a **Command Pattern** architecture:

1. **CLI Layer** (`bot.py`): Handles argument parsing and delegates to controllers
2. **Controller Layer** (`controller.py`): Orchestrates workflow execution and manages state
3. **Command Layer** (`command.py`): Implements individual operations (test, deploy, plan, etc.)
4. **Configuration Layer** (`config.py`): Manages bot configuration and validation

### Key Components

**GitHubCICDController**: Main orchestrator that:
- Manages GitHub API interactions via PyGithub
- Coordinates workflow execution across different commands
- Handles error reporting through GitHub Check Runs
- Manages PR comment interactions and status updates

**Command Implementations**:
- `run_tests()`: Executes unit tests with detailed reporting
- `update_pr_environment()`: Creates/updates virtual PR environments
- `gen_prod_plan()`: Generates production deployment plans
- `deploy_production()`: Handles production deployments
- `check_required_approvers()`: Validates approval requirements

**Configuration Management**:
- Uses Pydantic models for type-safe configuration
- Supports both YAML config files and environment variables
- Validates bot settings and user permissions
- Handles approval workflows and deployment triggers

### Integration with Core SQLMesh

The bot leverages core SQLMesh components:
- **Context**: Uses SQLMesh Context for project operations
- **Plan/Apply**: Integrates with SQLMesh's plan generation and application
- **Virtual Environments**: Creates isolated PR environments using SQLMesh's virtual data environments
- **State Sync**: Manages metadata synchronization across environments
- **Testing Framework**: Executes SQLMesh unit tests and reports results

### Error Handling and Reporting

- **GitHub Check Runs**: Creates detailed status reports for each workflow step
- **PR Comments**: Provides user-friendly feedback on failures and successes
- **Structured Logging**: Uses SQLMesh's logging framework for debugging
- **Exception Handling**: Graceful handling of GitHub API failures and SQLMesh errors

## Environment Variables for Engine Testing

When running engine-specific tests, these environment variables are required:

- **Snowflake**: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`
- **BigQuery**: `BIGQUERY_KEYFILE` or `GOOGLE_APPLICATION_CREDENTIALS`
- **Databricks**: `DATABRICKS_CATALOG`, `DATABRICKS_SERVER_HOSTNAME`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_ACCESS_TOKEN`, `DATABRICKS_CONNECT_VERSION`
- **Redshift**: `REDSHIFT_HOST`, `REDSHIFT_USER`, `REDSHIFT_PASSWORD`, `REDSHIFT_DATABASE`
- **Athena**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `ATHENA_S3_WAREHOUSE_LOCATION`
- **ClickHouse Cloud**: `CLICKHOUSE_CLOUD_HOST`, `CLICKHOUSE_CLOUD_USERNAME`, `CLICKHOUSE_CLOUD_PASSWORD`

## Migrations System

SQLMesh uses a migration system to evolve its internal state database schema and metadata format. The migrations handle changes to SQLMesh's internal structure, not user data transformations.

### Migration Structure

**Location**: `sqlmesh/migrations/` - Contains 80+ migration files from v0001 to v0083+

**Naming Convention**: `v{XXXX}_{descriptive_name}.py` (e.g., `v0001_init.py`, `v0083_use_sql_for_scd_time_data_type_data_hash.py`)

**Core Infrastructure**:
- `sqlmesh/core/state_sync/db/migrator.py`: Main migration orchestrator
- `sqlmesh/utils/migration.py`: Cross-database compatibility utilities
- `sqlmesh/core/state_sync/base.py`: Auto-discovery and loading logic

### Migration Categories

**Schema Evolution**:
- State table creation/modification (snapshots, environments, intervals)
- Column additions/removals and index management
- Database engine compatibility fixes (MySQL/MSSQL field size limits)

**Data Format Migrations**:
- JSON metadata structure updates (snapshot serialization changes)
- Path normalization (Windows compatibility)
- Fingerprint recalculation when SQLGlot parsing changes

**Cleanup Operations**:
- Removing obsolete tables and unused data
- Metadata optimization and attribute cleanup

### Key Migration Patterns

```python
# Standard migration function signature
def migrate(state_sync, **kwargs):  # type: ignore
    engine_adapter = state_sync.engine_adapter
    schema = state_sync.schema
    # Migration logic here

# Common operations
engine_adapter.create_state_table(table_name, columns_dict)
engine_adapter.alter_table(alter_expression)
engine_adapter.drop_table(table_name)
```

### State Management Integration

**Core State Tables**:
- `_snapshots`: Model version metadata (most frequently migrated)
- `_environments`: Environment definitions
- `_versions`: Schema/SQLGlot/SQLMesh version tracking
- `_intervals`: Incremental processing metadata

**Migration Safety**:
- Automatic backups before migration (unless `skip_backup=True`)
- Atomic database transactions for consistency
- Snapshot count validation before/after migrations
- Automatic rollback on failures

### Migration Execution

**Auto-Discovery**: Migrations are automatically loaded using `pkgutil.iter_modules()`

**Triggers**: Migrations run automatically when:
- Schema version mismatch detected
- SQLGlot version changes require fingerprint recalculation
- Manual `sqlmesh migrate` command execution

**Execution Flow**:
1. Version comparison (local vs remote schema)
2. Backup creation of state tables
3. Sequential migration execution (numerical order)
4. Snapshot fingerprint recalculation if needed
5. Environment updates with new snapshot references

## dbt Integration

SQLMesh provides native support for dbt projects, allowing users to run existing dbt projects while gaining access to SQLMesh's advanced features like virtual environments and plan/apply workflows.

### Core dbt Integration

**Location**: `sqlmesh/dbt/` - Complete dbt integration architecture

**Key Components**:
- `sqlmesh/dbt/loader.py`: Main dbt project loader extending SQLMesh's base loader
- `sqlmesh/dbt/manifest.py`: dbt manifest parsing and project discovery
- `sqlmesh/dbt/adapter.py`: dbt adapter system for SQL execution and schema operations
- `sqlmesh/dbt/model.py`: dbt model configurations and materialization mapping
- `sqlmesh/dbt/context.py`: dbt project context and environment management

### Project Conversion

**dbt Converter**: `sqlmesh/dbt/converter/` - Tools for migrating dbt projects to SQLMesh

**Key Features**:
- `convert.py`: Main conversion orchestration
- `jinja.py` & `jinja_transforms.py`: Jinja template and macro conversion
- Full support for dbt assets (models, seeds, sources, tests, snapshots, macros)

**CLI Commands**:
```bash
# Initialize SQLMesh in existing dbt project
sqlmesh init -t dbt

# Convert dbt project to SQLMesh format
sqlmesh dbt convert
```

### Supported dbt Features

**Project Structure**:
- Full dbt project support (models, seeds, sources, tests, snapshots, macros)
- dbt package dependencies and version management
- Profile integration using existing `profiles.yml` for connections

**Materializations**:
- All standard dbt materializations (table, view, incremental, ephemeral)
- Incremental model strategies (delete+insert, merge, insert_overwrite)
- SCD Type 2 support and snapshot strategies

**Advanced Features**:
- Jinja templating with full macro support
- Runtime variable passing and configuration
- dbt test integration and execution
- Cross-database compatibility with SQLMesh's multi-dialect support

### Example Projects

**sushi_dbt**: `examples/sushi_dbt/` - Complete dbt project running with SQLMesh
**Test Fixtures**: `tests/fixtures/dbt/sushi_test/` - Comprehensive test dbt project with all asset types

### Integration Benefits

When using dbt with SQLMesh, you gain:
- **Virtual Environments**: Isolated development without warehouse costs
- **Plan/Apply Workflow**: Safe deployments with change previews
- **Multi-Dialect Support**: Run the same dbt project across different SQL engines
- **Advanced Testing**: Enhanced testing capabilities beyond standard dbt tests
- **State Management**: Sophisticated metadata and versioning system
