import typing as t
from pathlib import Path
import shutil
import os

from sqlmesh.dbt.loader import sqlmesh_config, DbtLoader, DbtContext, Project
from sqlmesh.core.context import Context
import sqlmesh.core.dialect as d
from sqlmesh.core import constants as c

from sqlmesh.core.model.kind import SeedKind
from sqlmesh.core.model import SqlModel, SeedModel
from sqlmesh.dbt.converter.jinja import convert_jinja_query, convert_jinja_macro
from sqlmesh.dbt.converter.common import infer_dbt_package_from_path
import dataclasses
from dataclasses import dataclass

from sqlmesh.dbt.converter.console import DbtConversionConsole
from sqlmesh.utils.jinja import JinjaMacroRegistry, extract_macro_references_and_variables
from sqlmesh.utils import yaml


@dataclass
class ConversionReport:
    self_referencing_models: t.List[t.Tuple[Path, SqlModel]] = dataclasses.field(
        default_factory=list
    )


@dataclass
class InputPaths:
    # todo: read paths from DBT project yaml

    base: Path

    @property
    def models(self) -> Path:
        return self.base / "models"

    @property
    def seeds(self) -> Path:
        return self.base / "seeds"

    @property
    def tests(self) -> Path:
        return self.base / "tests"

    @property
    def macros(self) -> Path:
        return self.base / "macros"

    @property
    def snapshots(self) -> Path:
        return self.base / "snapshots"

    @property
    def packages(self) -> Path:
        return self.base / "dbt_packages"


@dataclass
class OutputPaths:
    base: Path

    @property
    def models(self) -> Path:
        return self.base / "models"

    @property
    def seeds(self) -> Path:
        return self.base / "seeds"

    @property
    def audits(self) -> Path:
        return self.base / "audits"

    @property
    def macros(self) -> Path:
        return self.base / "macros"


def convert_project_files(src: Path, dest: Path, no_prompts: bool = True) -> None:
    console = DbtConversionConsole()
    report = ConversionReport()

    console.log_message(f"Converting project at '{src}' to '{dest}'")

    ctx, dbt_project = _load_project(src)
    dbt_load_context = dbt_project.context

    console.start_project_conversion(src)

    input_paths, output_paths = _ensure_paths(src, dest, console, no_prompts)

    model_count = len(ctx.models)

    # DBT Models -> SQLMesh Models
    console.start_models_conversion(model_count)
    _convert_models(ctx, input_paths, output_paths, report, console)
    console.complete_models_conversion()

    # DBT Tests -> Standalone Audits
    console.start_audits_conversion(len(ctx.standalone_audits))
    _convert_standalone_audits(ctx, input_paths, output_paths, console)
    console.complete_audits_conversion()

    # DBT Macros -> SQLMesh Jinja Macros
    all_macros = list(
        iterate_macros(input_paths.macros, output_paths.macros, dbt_load_context, ctx)
    )
    console.start_macros_conversion(len(all_macros))
    for package, macro_text, input_id, output_file_path, should_transform in all_macros:
        console.start_macro_conversion(input_id)

        output_file_path.parent.mkdir(parents=True, exist_ok=True)
        converted = (
            convert_jinja_macro(ctx, macro_text, package) if should_transform else macro_text
        )
        output_file_path.write_text(converted, encoding="utf8")

        console.complete_macro_conversion()

    console.complete_macros_conversion()

    # Generate SQLMesh config
    # TODO: read all profiles from config and convert to gateways instead of just the current profile?
    console.log_message("Writing SQLMesh config")
    new_config = _generate_sqlmesh_config(ctx, dbt_project, dbt_load_context)
    (dest / "config.yml").write_text(yaml.dump(new_config))

    if report.self_referencing_models:
        console.output_report(report)

    console.log_message("All done")


def _load_project(src: Path) -> t.Tuple[Context, Project]:
    config = sqlmesh_config(project_root=src)

    ctx = Context(config=config, paths=src)

    dbt_loader = ctx._loaders[0]
    assert isinstance(dbt_loader, DbtLoader)

    dbt_project = dbt_loader._projects[0]

    return ctx, dbt_project


def _ensure_paths(
    src: Path, dest: Path, console: DbtConversionConsole, no_prompts: bool
) -> t.Tuple[InputPaths, OutputPaths]:
    if not dest.exists():
        console.log_message(f"Creating output directory: {dest}")
        dest.mkdir()

    if dest.is_file():
        raise ValueError(f"Output path must be a directory")

    if any(dest.iterdir()):
        if not no_prompts and console.prompt_clear_directory("Output directory ", dest):
            for path in dest.glob("**/*"):
                if path.is_file():
                    path.unlink()
                elif path.is_dir():
                    shutil.rmtree(path)
            console.log_message(f"Output directory '{dest}' cleared")
        else:
            raise ValueError("Please ensure the output directory is empty")

    input_paths = InputPaths(src)
    output_paths = OutputPaths(dest)

    for dir in (output_paths.models, output_paths.seeds, output_paths.audits, output_paths.macros):
        dir.mkdir()

    return input_paths, output_paths


def _convert_models(
    ctx: Context,
    input_paths: InputPaths,
    output_paths: OutputPaths,
    report: ConversionReport,
    console: DbtConversionConsole,
) -> None:
    # Iterating in DAG order helps minimize re-rendering when the fingerprint cache is busted when we call upsert_model() to check if
    # a self-referencing model has all its columns_to_types known or not
    for fqn in ctx.dag:
        model = ctx.models.get(fqn)

        if not model:
            # some entries in the dag are not models
            continue

        model_name = fqn

        # todo: support DBT model_paths[] being not `models` or being a list
        # todo: write out column_descriptions() into model block
        console.start_model_conversion(model_name)

        if model.kind.is_external:
            # skip external models
            # they can be created with `sqlmesh create_external_models` post-conversion
            console.complete_model_conversion()  # still advance the progress bar
            continue

        if model.kind.is_seed:
            # this will produce the original seed file, eg "items.csv"
            seed_filename = model._path.relative_to(input_paths.seeds)

            # seed definition - rename "items.csv" -> "items.sql"
            model_filename = seed_filename.with_suffix(".sql")

            # copy the seed data itself to the seeds dir
            shutil.copyfile(model._path, output_paths.seeds / seed_filename)

            # monkeypatch the model kind to have a relative reference to the seed file
            assert isinstance(model.kind, SeedKind)
            model.kind.path = str(Path("../seeds", seed_filename))
        else:
            if input_paths.models in model._path.parents:
                model_filename = model._path.relative_to(input_paths.models)
            elif input_paths.snapshots in model._path.parents:
                # /base/path/snapshots/foo.sql -> /output/path/models/dbt_snapshots/foo.sql
                model_filename = "dbt_snapshots" / model._path.relative_to(input_paths.snapshots)
            elif input_paths.packages in model._path.parents:
                model_filename = c.MIGRATED_DBT_PACKAGES / model._path.relative_to(
                    input_paths.packages
                )
            else:
                raise ValueError(f"Unhandled model path: {model._path}")

        # todo: a SQLGLot transform on `audits` in the model definition to lowercase the names?
        model_output_path = output_paths.models / model_filename
        model_output_path.parent.mkdir(parents=True, exist_ok=True)
        model_package = infer_dbt_package_from_path(model_output_path)

        def _render(e: d.exp.Expression) -> str:
            if isinstance(e, d.Jinja):
                e = convert_jinja_query(ctx, model, e, model_package)
            rendered = e.sql(dialect=model.dialect, pretty=True)
            if not isinstance(e, d.Jinja):
                rendered += ";"
            return rendered

        model_to_render = model.model_copy(
            update=dict(depends_on_=None if len(model.depends_on) > 0 else set())
        )
        if isinstance(model, (SqlModel, SeedModel)):
            # Keep depends_on for SQL Models because sometimes the entire query is a macro call.
            # If we clear it and rely on inference, the SQLMesh native loader will throw:
            # - ConfigError: Dependencies must be provided explicitly for models that can be rendered only at runtime
            model_to_render = model.model_copy(
                update=dict(depends_on_=resolve_fqns_to_model_names(ctx, model.depends_on))
            )

        rendered_queries = [
            _render(q)
            for q in model_to_render.render_definition(render_query=False, include_python=False)
        ]

        # add inline audits
        # todo: handle these better
        # maybe output generic audits for the 4 DBT audits (not_null, unique, accepted_values, relationships) and emit definitions for them?
        for _, audit in model.audit_definitions.items():
            rendered_queries.append("\n" + _render(d.parse_one(f"AUDIT (name {audit.name})")))
            # todo: or do we want the original?
            rendered_queries.append(_render(model.render_audit_query(audit)))

        model_definition = "\n".join(rendered_queries)

        model_output_path.write_text(model_definition)

        console.complete_model_conversion()


def _convert_standalone_audits(
    ctx: Context, input_paths: InputPaths, output_paths: OutputPaths, console: DbtConversionConsole
) -> None:
    for _, audit in ctx.standalone_audits.items():
        console.start_audit_conversion(audit.name)
        audit_definition = audit.render_definition(include_python=False)

        stringified = []
        for expression in audit_definition:
            if isinstance(expression, d.JinjaQuery):
                expression = convert_jinja_query(ctx, audit, expression)
            stringified.append(expression.sql(dialect=audit.dialect, pretty=True))

        audit_definition_string = ";\n".join(stringified)

        audit_filename = audit._path.relative_to(input_paths.tests)
        audit_output_path = output_paths.audits / audit_filename
        audit_output_path.write_text(audit_definition_string)
        console.complete_audit_conversion()
    return None


def _generate_sqlmesh_config(
    ctx: Context, dbt_project: Project, dbt_load_context: DbtContext
) -> t.Dict[str, t.Any]:
    DEFAULT_ARGS: t.Dict[str, t.Any]
    from sqlmesh.utils.pydantic import DEFAULT_ARGS

    base_config = ctx.config.model_dump(
        mode="json", include={"gateways", "model_defaults", "variables"}, **DEFAULT_ARGS
    )
    # Extend with the variables loaded from DBT
    if "variables" not in base_config:
        base_config["variables"] = {}
    if c.MIGRATED_DBT_PACKAGES not in base_config["variables"]:
        base_config["variables"][c.MIGRATED_DBT_PACKAGES] = {}

    # this is used when loading with the native loader to set the package name for top level macros
    base_config["variables"][c.MIGRATED_DBT_PROJECT_NAME] = dbt_project.context.project_name

    migrated_package_names = []
    for package in dbt_project.packages.values():
        dbt_load_context.set_and_render_variables(package.variables, package.name)

        if package.name == dbt_project.context.project_name:
            base_config["variables"].update(dbt_load_context.variables)
        else:
            base_config["variables"][c.MIGRATED_DBT_PACKAGES][package.name] = (
                dbt_load_context.variables
            )
            migrated_package_names.append(package.name)

    for package_name in migrated_package_names:
        # these entries are duplicates because the DBT loader already applies any project specific overrides to the
        # package level variables
        base_config["variables"].pop(package_name, None)

    return base_config


def iterate_macros(
    input_macros_dir: Path, output_macros_dir: Path, dbt_load_context: DbtContext, ctx: Context
) -> t.Iterator[t.Tuple[t.Optional[str], str, str, Path, bool]]:
    """
    Return an iterator over all the macros that need to be migrated

    The main project level ones are read from the source macros directory (it's assumed these are written by the user)

    The rest / library level ones are read from the DBT manifest based on merging together all the model JinjaMacroRegistry's from the SQLMesh context
    """

    all_macro_references = set()

    for dirpath, _, files in os.walk(
        input_macros_dir
    ):  # note: pathlib doesnt have a walk function until python 3.12
        for name in files:
            if name.lower().endswith(".sql"):
                input_file_path = Path(dirpath) / name

                output_file_path = output_macros_dir / (
                    input_file_path.relative_to(input_macros_dir)
                )

                input_file_contents = input_file_path.read_text(encoding="utf8")

                # as we migrate user-defined macros, keep track of other macros they reference from other packages/libraries
                # so we can be sure theyre included
                # (since there is no guarantee a model references a user-defined macro which means the dependencies may not be pulled in automatically)
                macro_refs, _ = extract_macro_references_and_variables(
                    input_file_contents, dbt_target_name=dbt_load_context.target_name
                )
                all_macro_references.update(macro_refs)

                yield (
                    None,
                    input_file_contents,
                    str(input_file_path),
                    output_file_path,
                    True,
                )

    jmr = JinjaMacroRegistry()
    for model in ctx.models.values():
        jmr = jmr.merge(model.jinja_macros)

    # add any macros that are referenced in user macros but not necessarily directly in models
    # this can happen if a user has defined a macro that is currently unused in a model but we still want to migrate it
    jmr = jmr.merge(
        dbt_load_context.jinja_macros.trim(
            all_macro_references, package=dbt_load_context.project_name
        )
    )

    for package, name, macro in jmr.all_macros:
        if package and package != dbt_load_context.project_name:
            output_file_path = output_macros_dir / c.MIGRATED_DBT_PACKAGES / package / f"{name}.sql"

            yield (
                package,
                macro.definition,
                f"{package}.{name}",
                output_file_path,
                "var(" in macro.definition,  # todo: check for ref() etc as well?
            )


def resolve_fqns_to_model_names(ctx: Context, fqns: t.Set[str]) -> t.Set[str]:
    # model.depends_on is provided by the DbtLoader as a list of fully qualified table name strings
    # if we output them verbatim, when loading them back we get errors like:
    # - ConfigError: Failed to load model definition: 'Dot' object has no attribute 'catalog'
    # So we need to resolve them to model names instead.
    # External models also need to be excluded because the "name" is still a FQN string so cause the above error

    return {
        ctx.models[i].name for i in fqns if i in ctx.models and not ctx.models[i].kind.is_external
    }
