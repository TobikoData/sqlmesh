from __future__ import annotations

import abc

from sqlmesh.core.model import Model

from typing import Type

import typing as t


if t.TYPE_CHECKING:
    from sqlmesh.core.context import GenericContext


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

    def __repr__(self) -> str:
        return self.name


class RuleViolation:
    def __init__(self, rule: Rule, violation_msg: str) -> None:
        self.rule = rule
        self.violation_msg = violation_msg

    def __repr__(self) -> str:
        return f"{self.rule.name}: {self.violation_msg}"
