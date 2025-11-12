# Customizing SQLMesh

SQLMesh supports the workflows used by the vast majority of data engineering teams. However, your company may have bespoke processes or tools that require special integration with SQLMesh.

Fortunately, SQLMesh is an open-source Python library, so you can view its underlying code and customize it for your needs.

Customization generally involves subclassing SQLMesh classes to extend or modify their functionality.

!!! danger "Caution"

    Customize SQLMesh with extreme caution. Errors may cause SQLMesh to produce unexpected results.

## Custom loader

Loading is the process of reading project files and converting their contents into SQLMesh's internal Python objects.

The loading stage is a convenient place to customize SQLMesh behavior because you can access a project's objects after they've been ingested from file but before SQLMesh uses them.

SQLMesh's `SqlMeshLoader` class handles the loading process - customize it by subclassing it and overriding its methods.

!!! note "Python configuration only"

    Custom loaders require using the [Python configuration format](./configuration.md#python) (YAML is not supported).

### Modify every model

One reason to customize the loading process is to do something to every model. For example, you might want to add a post-statement to every model.

The loading process parses all model SQL statements, so new or modified SQL must be parsed by SQLGlot before being passed to a model object.

This custom loader example adds a post-statement to every model:

``` python linenums="1" title="config.py"
from sqlmesh.core.loader import SqlMeshLoader
from sqlmesh.utils import UniqueKeyDict
from sqlmesh.core.dialect import parse_one
from sqlmesh.core.config import Config

# New `CustomLoader` class subclasses `SqlMeshLoader`
class CustomLoader(SqlMeshLoader):
    # Override SqlMeshLoader's `_load_models` method to access every model
    def _load_models(
        self,
        macros: "MacroRegistry",
        jinja_macros: "JinjaMacroRegistry",
        gateway: str | None,
        audits: UniqueKeyDict[str, "ModelAudit"],
        signals: UniqueKeyDict[str, "signal"],
    ) -> UniqueKeyDict[str, "Model"]:
        # Call SqlMeshLoader's normal `_load_models` method to ingest models from file and parse model SQL
        models = super()._load_models(macros, jinja_macros, gateway, audits, signals)

        new_models = {}
        # Loop through the existing model names/objects
        for model_name, model in models.items():
            # Create list of existing and new post-statements
            new_post_statements = [
                # Existing post-statements from model object
                *model.post_statements,
                # New post-statement is raw SQL, so we parse it with SQLGlot's `parse_one` function.
                # Make sure to specify the SQL dialect if different from the project default.
                parse_one(f"VACUUM @this_model"),
            ]
            # Create a copy of the model with the `post_statements_` field updated
            new_models[model_name] = model.copy(update={"post_statements_": new_post_statements})

        return new_models

# Pass the CustomLoader class to the SQLMesh configuration object
config = Config(
    # < your configuration parameters here >,
    loader=CustomLoader,
)
```