# Project guide

## Creating a project

---

Before getting started, ensure that you meet the [prerequisites](../prerequisites.md) for using SQLMesh.

---

To create a project from the command line, follow these steps:

1. Create a directory for your project:

    ```bash
    mkdir my-project
    ```

2. Change directories into your new project:

    ```bash
    cd my-project
    ```

    From here, you can create your project structure from scratch, or SQLMesh can scaffold one for you. For the purposes of this guide, we'll show you how to scaffold your project so that you can get up and running quickly.

1. To scaffold a project, it is recommended that you use a python virtual environment by running the following commands:

    ```bash
    python -m venv .env
    ```

    ```bash
    source .env/bin/activate
    ```

    ```bash
    pip install sqlmesh
    ```

    **Note:** When using a python virtual environment, you must ensure that it is activated first. You should see `(.env)` in your command line; if you don't, run `source .env/bin/activate` from your project directory to activate your environment.

1. Once you have activated your environment, run the following command and SQLMesh will build out your project:

    ```bash
    sqlmesh init
    ```

    The following directories and files will be created that you can use to organize your SQLMesh project:

    - config.py (database configuration file)
    - ./models (SQL and Python models)
    - ./audits (shared audits)
    - ./tests (unit tests)
    - ./macros

## Editing an existing project

To edit an existing project, open the project file you wish to edit in your preferred editor.

If using CLI or Notebook, you can open a file in your project for editing by using the `sqlmesh` command with the `-p` variable, and pointing to your project's path as follows:

```bash
sqlmesh -p <your-project-path>
```

For more details, refer to [CLI](../reference/cli.md) and [Notebook](../reference/notebook.md).

## Importing a project

### dbt

To import a dbt project, use the `sqlmesh init` command with the `dbt` flag as follows:

```bash
sqlmesh init -t dbt
```
