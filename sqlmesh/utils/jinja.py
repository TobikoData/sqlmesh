from __future__ import annotations

import importlib
import re
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
    extensions.append("jinja2.ext.loopcontrols")
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
    return ENVIRONMENT.from_string(query).render(methods or {})


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
        root_package_name: The name of the root package. If specified root macros will be available
            as both `root_package_name.macro_name` and `macro_name`.
        top_level_packages: The list of top-level packages. Macros in this packages will be available
            as both `package_name.macro_name` and `macro_name`.
    """

    packages: t.Dict[str, t.Dict[str, MacroInfo]] = {}
    root_macros: t.Dict[str, MacroInfo] = {}
    global_objs: t.Dict[str, JinjaGlobalAttribute] = {}
    create_builtins_module: t.Optional[str] = None
    root_package_name: t.Optional[str] = None
    top_level_packages: t.List[str] = []

    _parser_cache: t.Dict[t.Tuple[t.Optional[str], str], Template] = {}
    __environment: t.Optional[Environment] = None

    @validator("global_objs", pre=True)
    def _validate_attribute_dict(cls, value: t.Any) -> t.Any:
        def _attribute_dict(val: t.Dict[str, t.Any]) -> AttributeDict:
            return AttributeDict(
                {k: _attribute_dict(v) if isinstance(v, dict) else v for k, v in val.items()}
            )

        if isinstance(value, t.Dict):
            return _attribute_dict(value)
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
        env: Environment = self.build_environment(**kwargs)
        if reference.package is not None:
            package = env.globals.get(reference.package, {})
            return package.get(reference.name)  # type: ignore
        return env.globals.get(reference.name)  # type: ignore

    def build_environment(self, **kwargs: t.Any) -> Environment:
        """Builds a new Jinja environment based on this registry."""

        context: t.Dict[str, t.Any] = {}

        root_macros = {
            name: self._MacroWrapper(name, None, self, context)
            for name, macro in self.root_macros.items()
        }

        package_macros: t.Dict[str, t.Any] = defaultdict(AttributeDict)
        for package_name, macros in self.packages.items():
            for macro_name in macros:
                package_macros[package_name][macro_name] = self._MacroWrapper(
                    macro_name, package_name, self, context
                )

        if self.root_package_name is not None:
            package_macros[self.root_package_name].update(root_macros)

        for top_level_package_name in self.top_level_packages:
            root_macros.update(package_macros.get(top_level_package_name, {}))

        env = environment()

        context.update(root_macros)
        context.update(package_macros)
        context.update(self._create_builtin_globals(kwargs))

        env.globals.update(context)
        env.filters.update(self._environment.filters)
        return env

    def trim(
        self, dependencies: t.Iterable[MacroReference], package: t.Optional[str] = None
    ) -> JinjaMacroRegistry:
        """Trims the registry by keeping only macros with given references and their transitive dependencies.

        Args:
            dependencies: References to macros that should be kept.
            package: The name of the package in the context of which the trimming should be performed.

        Returns:
            A new trimmed registry.
        """
        dependencies_by_package: t.Dict[t.Optional[str], t.Set[str]] = defaultdict(set)
        for dep in dependencies:
            dependencies_by_package[dep.package or package].add(dep.name)

        top_level_packages = self.top_level_packages.copy()
        if package is not None:
            top_level_packages.append(package)

        result = JinjaMacroRegistry(
            global_objs=self.global_objs.copy(),
            create_builtins_module=self.create_builtins_module,
            root_package_name=self.root_package_name,
            top_level_packages=top_level_packages,
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
            root_package_name=self.root_package_name or other.root_package_name,
            top_level_packages=[*self.top_level_packages, *other.top_level_packages],
        )

    def __deepcopy__(self, memo: t.Dict[int, t.Any]) -> JinjaMacroRegistry:
        return JinjaMacroRegistry.parse_obj(self.dict())

    def _parse_macro(self, name: str, package: t.Optional[str]) -> Template:
        cache_key = (package, name)
        if cache_key not in self._parser_cache:
            macro = self._get_macro(name, package)

            definition: nodes.Template = self._environment.parse(macro.definition)
            if _is_private_macro(name):
                # A workaround to expose private jinja macros.
                definition = self._to_non_private_macro_def(name, definition)

            self._parser_cache[cache_key] = self._environment.from_string(definition)
        return self._parser_cache[cache_key]

    @property
    def _environment(self) -> Environment:
        if self.__environment is None:
            self.__environment = environment()
            self.__environment.filters.update(self._create_builtin_filters())
        return self.__environment

    def _trim_macros(
        self,
        names: t.Set[str],
        package: t.Optional[str],
        visited: t.Optional[t.Dict[t.Optional[str], t.Set[str]]] = None,
    ) -> JinjaMacroRegistry:
        if visited is None:
            visited = defaultdict(set)

        macros = self.packages.get(package, {}) if package is not None else self.root_macros
        trimmed_macros = {}

        dependencies: t.Dict[t.Optional[str], t.Set[str]] = defaultdict(set)

        for name in names:
            if name in macros and name not in visited[package]:
                macro = macros[name]
                trimmed_macros[name] = macro
                for dependency in macro.depends_on:
                    dependencies[dependency.package or package].add(dependency.name)
                visited[package].add(name)

        if package is not None:
            result = JinjaMacroRegistry(packages={package: trimmed_macros})
        else:
            result = JinjaMacroRegistry(root_macros=trimmed_macros)

        for upstream_package, upstream_names in dependencies.items():
            result = result.merge(
                self._trim_macros(upstream_names, upstream_package, visited=visited)
            )

        return result

    def _macro_exists(self, name: str, package: t.Optional[str]) -> bool:
        return (
            name in self.packages.get(package, {})
            if package is not None
            else name in self.root_macros
        )

    def _get_macro(self, name: str, package: t.Optional[str]) -> MacroInfo:
        return self.packages[package][name] if package is not None else self.root_macros[name]

    def _to_non_private_macro_def(self, name: str, template: nodes.Template) -> nodes.Template:
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

    class _MacroWrapper:
        def __init__(
            self,
            name: str,
            package: t.Optional[str],
            registry: JinjaMacroRegistry,
            context: t.Dict[str, t.Any],
        ):
            self.name = name
            self.package = package
            self.context = context
            self.registry = registry

        def __call__(self, *args: t.Any, **kwargs: t.Any) -> t.Any:
            context = self.context.copy()
            if self.package is not None and self.package in context:
                context.update(context[self.package])

            template = self.registry._parse_macro(self.name, self.package)
            macro_callable = getattr(
                template.make_module(vars=context), _non_private_name(self.name)
            )
            try:
                return macro_callable(*args, **kwargs)
            except MacroReturnVal as ret:
                return ret.value


def _is_private_macro(name: str) -> bool:
    return name.startswith("_")


def _non_private_name(name: str) -> str:
    return name.lstrip("_")


JINJA_REGEX = re.compile(r"({{|{%)")


def has_jinja(value: str) -> bool:
    return JINJA_REGEX.search(value) is not None
