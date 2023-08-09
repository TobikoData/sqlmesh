# Overview

SQL is a [declarative language](https://en.wikipedia.org/wiki/Declarative_programming). It does not natively have features like variables or control flow logic (if-then, for loops) that allow SQL commands to behave differently in different situations.

However, data pipelines are dynamic and need different behavior depending on context. SQL is made dynamic with *macros*.

SQLMesh supports two macro systems: SQLMesh macros and the [Jinja](https://jinja.palletsprojects.com/en/3.1.x/) templating system.

Learn more about macros in SQLMesh:

- [Pre-defined macro variables](./macro_variables.md) available in both macro systems
- [SQLMesh macros](./sqlmesh_macros.md)
- [Jinja macros](./jinja_macros.md)
