# Serialization

SQLMesh executes Python code through [macros](../macros/overview.md) and [Python models](../../concepts/models/python_models.md). Each Python model is stored as a standalone [snapshot](../architecture/snapshots.md), which includes all of the Python code necessary to generate it.

## Serialization format

Rather than using Python's `pickle` format, SQLMesh has it's own serialization format. This is because `pickle` is not compatible across Python versions, and would, for example, prevent you from developing on Python 3.7 and then running Python 3.8 in production.

Instead, SQLMesh stores the string representation of your Python implementation and then re-evaluates it. Given a custom Python function or macro, SQLMesh reads the Abstract Syntax Tree (AST) of the function and converts that into a string representation, along with all dependencies and global variables. For more information, refer to [snapshot fingerprinting](../architecture/snapshots.md#fingerprinting).

### Limitations

SQLMesh only serializes the Python code you write and does not include libraries, which means the module of your code must match your SQLMesh config path. In addition, any references to libraries will be converted to imports, so you must ensure that any libraries you are using are installed everywhere that SQLMesh is running.
