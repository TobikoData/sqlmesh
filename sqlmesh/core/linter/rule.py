from __future__ import annotations

import abc

from sqlmesh.core.model import Model

from typing import Type

import typing as t

from sqlmesh.utils.pydantic import PydanticModel


if t.TYPE_CHECKING:
    from sqlmesh.core.context import GenericContext


class RuleLocation(PydanticModel):
    """The location of a rule in a file."""

    file_path: str
    start_line: t.Optional[int] = None


class _Rule(abc.ABCMeta):
    def __new__(cls: Type[_Rule], clsname: str, bases: t.Tuple, attrs: t.Dict) -> _Rule:
        attrs["name"] = clsname.lower()
        return super().__new__(cls, clsname, bases, attrs)


class Rule(abc.ABC, metaclass=_Rule):
    """The base class for a rule."""

    name = "rule"

    def __init__(self, context: GenericContext):
        self.context = context

    @abc.abstractmethod
    def check_model(self, model: Model) -> t.Optional[RuleViolation]:
        """The evaluation function that'll check for a violation of this rule."""

    @property
    def summary(self) -> str:
        """A summary of what this rule checks for."""
        return self.__doc__ or ""

    def violation(self, violation_msg: t.Optional[str] = None) -> RuleViolation:
        """Create a RuleViolation instance for this rule"""
        return RuleViolation(rule=self, violation_msg=violation_msg or self.summary)

    def get_definition_location(self) -> RuleLocation:
        """Return the file path and position information for this rule.

        This method returns information about where this rule is defined,
        which can be used in diagnostics to link to the rule's documentation.

        Returns:
            A dictionary containing file path and position information.
        """
        import inspect

        # Get the file where the rule class is defined
        file_path = inspect.getfile(self.__class__)

        try:
            # Get the source code and line number
            source_lines, start_line = inspect.getsourcelines(self.__class__)
            return RuleLocation(
                file_path=file_path,
                start_line=start_line,
            )
        except (IOError, TypeError):
            # Fall back to just returning the file path if we can't get source lines
            return RuleLocation(file_path=file_path)

    def __repr__(self) -> str:
        return self.name


class RuleViolation:
    def __init__(self, rule: Rule, violation_msg: str) -> None:
        self.rule = rule
        self.violation_msg = violation_msg

    def __repr__(self) -> str:
        return f"{self.rule.name}: {self.violation_msg}"
