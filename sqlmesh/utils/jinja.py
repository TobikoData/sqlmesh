from __future__ import annotations

import typing as t
from collections import defaultdict

from jinja2 import Environment, Template, nodes
from jinja2.environment import TemplateModule
from sqlglot import Dialect, Parser, TokenType

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
            if self._at_block_start():
                if self._prev and self._prev.token_type == TokenType.L_BRACE:
                    self._advance()
                macro_start = self._curr
            elif self._tag == "MACRO" and self._next:
                name = self._next.text
                while self._curr and not self._at_block_end():
                    self._advance()
                else:
                    if self._prev and self._prev.token_type == TokenType.R_BRACE:
                        self._advance()

                while self._curr and self._tag != "ENDMACRO":
                    if self._at_block_start():
                        if self._prev and self._prev.token_type == TokenType.L_BRACE:
                            self._advance()

                    self._advance()

                macro_str = self._find_sql(macro_start, self._next)
                macros[name] = MacroInfo(
                    definition=macro_str,
                    depends_on=list(extract_macro_references(macro_str)),
                )

            self._advance()

        return macros

    def _at_block_start(self) -> bool:
        return self._curr.token_type == TokenType.BLOCK_START or self._match_pair(
            TokenType.L_BRACE, TokenType.L_BRACE, advance=False
        )

    def _at_block_end(self) -> bool:
        return self._curr.token_type == TokenType.BLOCK_END or self._match_pair(
            TokenType.R_BRACE, TokenType.R_BRACE, advance=False
        )

    def _advance(self, times: int = 1) -> None:
        super()._advance(times)
        self._tag = (
            self._curr.text.upper()
            if self._curr
            and self._prev
            and (
                self._prev.token_type == TokenType.BLOCK_START
                or (
                    self._index > 1
                    and self._tokens[self._index - 1].token_type == TokenType.L_BRACE
                    and self._tokens[self._index - 2].token_type == TokenType.L_BRACE
                )
            )
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


class JinjaMacroRegistry(PydanticModel):
    packages: t.Dict[str, t.Dict[str, MacroInfo]] = {}
    root_macros: t.Dict[str, MacroInfo] = {}

    _parser_cache: t.Dict[t.Optional[str], Template] = {}
    __environment: t.Optional[Environment] = None

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

    def get_macro(self, reference: MacroReference) -> t.Optional[MacroInfo]:
        """Returns a macro for a given reference.

        Args:
            reference: The macro reference.

        Returns:
            The macro or None if not found.
        """
        if reference.package is not None:
            return self.packages.get(reference.package, {}).get(reference.name)
        return self.root_macros.get(reference.name)

    def build_environment(self, **kwargs: t.Any) -> Environment:
        """Builds a new Jinja environment based on this registry."""
        root_module = self._make_module(None, **kwargs)
        package_modules = {name: self._make_module(name, **kwargs) for name in self.packages}

        env = environment()
        env.globals.update(
            {
                **package_modules,
                **{
                    name: getattr(root_module, name)
                    for name in dir(root_module)
                    if not name.startswith("_")
                },
                **kwargs,
            }
        )
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

        result = JinjaMacroRegistry()
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

        return JinjaMacroRegistry(packages=packages, root_macros=root_macros)

    def _make_module(self, package: t.Optional[str], **kwargs: t.Any) -> TemplateModule:
        module_vars = kwargs.copy()
        template = self._parse_package(package)

        macros = self.packages[package] if package is not None else self.root_macros
        upstream_packages = {
            ref.package
            for macro in macros.values()
            for ref in macro.depends_on
            if ref.package != package and ref.package in self.packages
        }
        for upstream_package in upstream_packages:
            module_vars[upstream_package] = self._make_module(upstream_package, **kwargs)

        if package is not None:
            # Make sure that package can reference self.
            module_vars[package] = template.make_module(vars=module_vars)

        return template.make_module(vars=module_vars)

    def _parse_package(self, package: t.Optional[str]) -> Template:
        if package not in self._parser_cache:
            macros = self.packages[package] if package is not None else self.root_macros
            package_content = "\n".join(macro.definition for macro in macros.values())
            self._parser_cache[package] = self._environment.from_string(package_content)
        return self._parser_cache[package]

    @property
    def _environment(self) -> Environment:
        if self.__environment is None:
            self.__environment = environment()
        return self.__environment

    def _trim_macros(self, names: t.Set[str], package: t.Optional[str]) -> JinjaMacroRegistry:
        macros = self.packages[package] if package is not None else self.root_macros
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
