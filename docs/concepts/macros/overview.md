# Overview

SQL is a static language. It does not have features like variables or control flow logic (if-then) that allow SQL commands to behave differently in different situations.

However, data pipelines are dynamic and need different behavior depending on context. SQL is made dynamic with *macros*. 

SQLMesh supports two macro systems: SQLMesh macros and the [Jinja](https://jinja.palletsprojects.com/en/3.1.x/) templating system.

Learn more about macros in SQLMesh:

- [Pre-defined macro variables](./macro_variables.md) available in both macro systems
- [SQLMesh macros](./sqlmesh_macros.md)
- [Jinja macros](./jinja_macros.md)
