from __future__ import annotations

import importlib
import typing as t
from collections import defaultdict

from jinja2 import Environment, Template, nodes
from pydantic import validator
from sqlglot import Dialect, Parser, TokenType

from sqlmesh.utils import AttributeDict
from sqlmesh.utils.pydantic import PydanticModel


def environment(**kwargs: t.Any) -> Environment:
    extensions = kwargs.pop("extensions", [])
    extensions.append("jinja2.ext.do")
    return Environment(extensions=extensions, **kwargs)


ENVIRONMENT = environment()


class MacroReference(PydanticModel, frozen=True):
    package: t.Optional[str]
    name: str

    @property
    def reference(self) -> str:
        if self.package is None:
            return self.name
        return ".".join((self.package, self.name))

    def __str__(self) -> str:
        return self.reference


class MacroInfo(PydanticModel):
    """Class to hold macro and its calls"""

    definition: str
    depends_on: t.List[MacroReference]


class MacroReturnVal(Exception):
    def __init__(self, val: t.Any):
        self.value = val


def macro_return(macro: t.Callable) -> t.Callable:
    """Decorator to pass data back to the caller"""

    def wrapper(*args: t.Any, **kwargs: t.Any) -> t.Any:
        try:
            return macro(*args, **kwargs)
        except MacroReturnVal as ret:
            return ret.value

    return wrapper


class MacroExtractor(Parser):
    def extract(self, jinja: str, dialect: str = "") -> t.Dict[str, MacroInfo]:
        """Extract a dictionary of macro definitions from a jinja string.

        Args:
            jinja: The jinja string to extract from.
            dialect: The dialect of SQL.

        Returns:
            A dictionary of macro name to macro definition.
        """
        self.reset()
        self.sql = jinja
        self._tokens = Dialect.get_or_raise(dialect)().tokenizer.tokenize(jinja)
        self._index = -1
        self._advance()

        macros: t.Dict[str, MacroInfo] = {}

        while self._curr:
            if self._curr.token_type == TokenType.BLOCK_START:
                macro_start = self._curr
            elif self._tag == "MACRO" and self._next:
                name = self._next.text
                while self._curr and self._curr.token_type != TokenType.BLOCK_END:
                    self._advance()

                while self._curr and self._tag != "ENDMACRO":
                    self._advance()

                macro_str = self._find_sql(macro_start, self._next)
                macros[name] = MacroInfo(
                    definition=macro_str,
                    depends_on=list(extract_macro_references(macro_str)),
                )

            self._advance()

        return macros

    def _advance(self, times: int = 1) -> None:
        super()._advance(times)
        self._tag = (
            self._curr.text.upper()
            if self._curr and self._prev and self._prev.token_type == TokenType.BLOCK_START
            else ""
        )


def call_name(node: nodes.Expr) -> t.Tuple[str, ...]:
    if isinstance(node, nodes.Name):
        return (node.name,)
    if isinstance(node, nodes.Const):
        return (f"'{node.value}'",)
    if isinstance(node, nodes.Getattr):
        return call_name(node.node) + (node.attr,)
    if isinstance(node, (nodes.Getitem, nodes.Call)):
        return call_name(node.node)
    return ()


def render_jinja(query: str, methods: t.Optional[t.Dict[str, t.Any]] = None) -> str:
    return ENVIRONMENT.from_string(query).render(methods)


def find_call_names(node: nodes.Node, vars_in_scope: t.Set[str]) -> t.Iterator[t.Tuple[str, ...]]:
    vars_in_scope = vars_in_scope.copy()
    for child_node in node.iter_child_nodes():
        if "target" in child_node.fields:
            target = getattr(child_node, "target")
            if isinstance(target, nodes.Name):
                vars_in_scope.add(target.name)
            elif isinstance(target, nodes.Tuple):
                for item in target.items:
                    if isinstance(item, nodes.Name):
                        vars_in_scope.add(item.name)
        elif isinstance(child_node, nodes.Macro):
            for arg in child_node.args:
                vars_in_scope.add(arg.name)
        elif isinstance(child_node, nodes.Call):
            name = call_name(child_node)
            if name[0][0] != "'" and name[0] not in vars_in_scope:
                yield name
        yield from find_call_names(child_node, vars_in_scope)


def extract_call_names(jinja_str: str) -> t.List[t.Tuple[str, ...]]:
    return list(find_call_names(ENVIRONMENT.parse(jinja_str), set()))


def extract_macro_references(jinja_str: str) -> t.Set[MacroReference]:
    result = set()
    for call_name in extract_call_names(jinja_str):
        if len(call_name) == 1:
            result.add(MacroReference(name=call_name[0]))
        elif len(call_name) == 2:
            result.add(MacroReference(package=call_name[0], name=call_name[1]))
    return result


JinjaGlobalAttribute = t.Union[str, int, float, bool, AttributeDict]


class JinjaMacroRegistry(PydanticModel):
    """Registry for Jinja macros.

    Args:
        packages: The mapping from package name to a collection of macro definitions.
        root_macros: The collection of top-level macro definitions.
        global_objs: The global objects.
        create_builtins_module: The name of a module which defines the `create_builtins` factory
            function that will be used to construct builtin variables and functions.
    """

    packages: t.Dict[str, t.Dict[str, MacroInfo]] = {}
    root_macros: t.Dict[str, MacroInfo] = {}
    global_objs: t.Dict[str, JinjaGlobalAttribute] = {}
    create_builtins_module: t.Optional[str] = None

    _parser_cache: t.Dict[t.Tuple[t.Optional[str], str], Template] = {}
    __environment: t.Optional[Environment] = None

    @validator("global_objs", pre=True)
    def _validate_attribute_dict(cls, value: t.Any) -> t.Any:
        if isinstance(value, t.Dict):
            return {k: AttributeDict(v) if isinstance(v, dict) else v for k, v in value.items()}
        return value

    def add_macros(self, macros: t.Dict[str, MacroInfo], package: t.Optional[str] = None) -> None:
        """Adds macros to the target package.

        Args:
            macros: Macros that should be added.
            package: The name of the package the given macros belong to. If not specified, the provided
            macros will be added to the root namespace.
        """

        if package is not None:
            package_macros = self.packages.get(package, {})
            package_macros.update(macros)
            self.packages[package] = package_macros
        else:
            self.root_macros.update(macros)

    def build_macro(self, reference: MacroReference, **kwargs: t.Any) -> t.Optional[t.Callable]:
        """Builds a Python callable for a macro with the given reference.

        Args:
            reference: The macro reference.
        Returns:
            The macro as a Python callable or None if not found.
        """
        if reference.package is not None and reference.name not in self.packages.get(
            reference.package, {}
        ):
            return None
        if reference.package is None and reference.name not in self.root_macros:
            return None

        global_vars = self._create_builtin_globals(kwargs)
        return self._make_callable(reference.name, reference.package, {}, global_vars)

    def build_environment(self, **kwargs: t.Any) -> Environment:
        """Builds a new Jinja environment based on this registry."""

        global_vars = self._create_builtin_globals(kwargs)

        callable_cache: t.Dict[t.Tuple[t.Optional[str], str], t.Callable] = {}

        root_macros = {
            name: self._make_callable(name, None, callable_cache, global_vars)
            for name, macro in self.root_macros.items()
            if not _is_private_macro(name)
        }

        package_macros: t.Dict[str, t.Any] = defaultdict(AttributeDict)
        for package_name, macros in self.packages.items():
            for macro_name, macro in macros.items():
                if not _is_private_macro(macro_name):
                    package_macros[package_name][macro_name] = self._make_callable(
                        macro_name, package_name, callable_cache, global_vars
                    )

        env = environment()
        env.globals.update(
            {
                **root_macros,
                **package_macros,
                **global_vars,
            }
        )
        env.filters.update(self._environment.filters)
        return env

    def trim(self, dependencies: t.Iterable[MacroReference]) -> JinjaMacroRegistry:
        """Trims the registry by keeping only macros with given references and their transitive dependencies.

        Args:
            dependencies: References to macros that should be kept.

        Returns:
            A new trimmed registry.
        """
        dependencies_by_package: t.Dict[t.Optional[str], t.Set[str]] = defaultdict(set)
        for dep in dependencies:
            dependencies_by_package[dep.package].add(dep.name)

        result = JinjaMacroRegistry(
            global_objs=self.global_objs.copy(), create_builtins_module=self.create_builtins_module
        )
        for package, names in dependencies_by_package.items():
            result = result.merge(self._trim_macros(names, package))

        return result

    def merge(self, other: JinjaMacroRegistry) -> JinjaMacroRegistry:
        """Returns a copy of the registry which contains macros from both this and `other` instances.

        Args:
            other: The other registry instance.

        Returns:
            A new merged registry.
        """

        root_macros = {
            **self.root_macros,
            **other.root_macros,
        }

        packages = {}
        for package in {*self.packages, *other.packages}:
            packages[package] = {
                **self.packages.get(package, {}),
                **other.packages.get(package, {}),
            }

        global_objs = {
            **self.global_objs,
            **other.global_objs,
        }

        return JinjaMacroRegistry(
            packages=packages,
            root_macros=root_macros,
            global_objs=global_objs,
            create_builtins_module=self.create_builtins_module or other.create_builtins_module,
        )

    def _make_callable(
        self,
        name: str,
        package: t.Optional[str],
        callable_cache: t.Dict[t.Tuple[t.Optional[str], str], t.Callable],
        macro_vars: t.Dict[str, t.Any],
    ) -> t.Callable:
        cache_key = (package, name)
        if cache_key in callable_cache:
            return callable_cache[cache_key]

        macro_vars = macro_vars.copy()
        macro = self._get_macro(name, package)

        package_macros: t.Dict[str, AttributeDict] = defaultdict(AttributeDict)
        for dependency in macro.depends_on:
            if (dependency.package is None and dependency.name == name) or not self._macro_exists(
                dependency.name, dependency.package or package
            ):
                continue

            upstream_callable = self._make_callable(
                dependency.name, dependency.package or package, callable_cache, macro_vars
            )
            if dependency.package is None:
                macro_vars[dependency.name] = upstream_callable
            else:
                package_macros[dependency.package][dependency.name] = upstream_callable

        macro_vars.update(package_macros)

        template = self._parse_macro(name, package)
        macro_callable = macro_return(
            getattr(template.make_module(vars=macro_vars), _non_private_name(name))
        )
        callable_cache[cache_key] = macro_callable
        return macro_callable

    def _parse_macro(self, name: str, package: t.Optional[str]) -> Template:
        cache_key = (package, name)
        if cache_key not in self._parser_cache:
            macro = self._get_macro(name, package)

            definition: t.Union[str, nodes.Template] = macro.definition
            if _is_private_macro(name):
                # A workaround to expose private jinja macros.
                definition = self._to_non_private_macro_def(name, macro.definition)

            self._parser_cache[cache_key] = self._environment.from_string(definition)
        return self._parser_cache[cache_key]

    @property
    def _environment(self) -> Environment:
        if self.__environment is None:
            self.__environment = environment()
            self.__environment.filters.update(self._create_builtin_filters())
        return self.__environment

    def _trim_macros(self, names: t.Set[str], package: t.Optional[str]) -> JinjaMacroRegistry:
        macros = self.packages.get(package, {}) if package is not None else self.root_macros
        trimmed_macros = {}

        dependencies: t.Dict[t.Optional[str], t.Set[str]] = defaultdict(set)

        for name in names:
            if name in macros:
                macro = macros[name]
                trimmed_macros[name] = macro
                for dependency in macro.depends_on:
                    dependencies[dependency.package or package].add(dependency.name)

        if package is not None:
            result = JinjaMacroRegistry(packages={package: trimmed_macros})
        else:
            result = JinjaMacroRegistry(root_macros=trimmed_macros)

        for upstream_package, upstream_names in dependencies.items():
            result = result.merge(self._trim_macros(upstream_names, upstream_package))

        return result

    def _macro_exists(self, name: str, package: t.Optional[str]) -> bool:
        return (
            name in self.packages.get(package, {})
            if package is not None
            else name in self.root_macros
        )

    def _get_macro(self, name: str, package: t.Optional[str]) -> MacroInfo:
        return self.packages[package][name] if package is not None else self.root_macros[name]

    def _to_non_private_macro_def(self, name: str, definition: str) -> nodes.Template:
        template = self._environment.parse(definition)

        for node in template.find_all((nodes.Macro, nodes.Call)):
            if isinstance(node, nodes.Macro):
                node.name = _non_private_name(name)
            elif isinstance(node, nodes.Call) and isinstance(node.node, nodes.Name):
                node.node.name = _non_private_name(name)

        return template

    def _create_builtin_globals(self, global_vars: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
        """Creates Jinja builtin globals using a factory function defined in the provided module."""
        engine_adapter = global_vars.pop("engine_adapter", None)
        global_vars = {**self.global_objs, **global_vars}
        if self.create_builtins_module is not None:
            module = importlib.import_module(self.create_builtins_module)
            if hasattr(module, "create_builtin_globals"):
                return module.create_builtin_globals(self, global_vars, engine_adapter)
        return global_vars

    def _create_builtin_filters(self) -> t.Dict[str, t.Any]:
        """Creates Jinja builtin filters using a factory function defined in the provided module."""
        if self.create_builtins_module is not None:
            module = importlib.import_module(self.create_builtins_module)
            if hasattr(module, "create_builtin_filters"):
                return module.create_builtin_filters()
        return {}


def _is_private_macro(name: str) -> bool:
    return name.startswith("_")


def _non_private_name(name: str) -> str:
    return name.lstrip("_")
