# Prerequisites

This page describes the system prerequisites needed to run SQLMesh and provides instructions for meeting them.

## SQLMesh prerequisites

You'll need Python 3.8 or higher to use SQLMesh. You can check your python version by running the following command:
```bash
python3 --version
```

or:

```bash
python --version
```

**Note:** If `python --version` returns 2.x, replace all `python` commands with `python3`, and `pip` with `pip3`.

## Additional prerequisites for integrations

If integrating with Airflow, you'll also need to install the SQLMesh Python package on all nodes of the Airflow cluster. For more information, refer to [Integrate with Airflow](./guides/scheduling.md#integrating-with-airflow).

## Next steps

Now that your machine meets the prerequisites, [install SQLMesh](installation.md).
