from __future__ import annotations

import abc

import operator as op
from collections.abc import Iterator, Iterable, Set, Mapping, Callable
from functools import reduce

from sqlmesh.core.model import Model

import typing as t


class Rule(abc.ABC):
    """The base class for a rule."""

    def __init__(self) -> None:
        self._name = self.__class__.__name__.lower()

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

    @property
    def name(self) -> str:
        """The name of this rule."""
        return self._name

    def __repr__(self) -> str:
        return self.name


class RuleViolation:
    def __init__(self, rule: Rule, violation_msg: str) -> None:
        self.rule = rule
        self.violation_msg = violation_msg

    def __repr__(self) -> str:
        return f"{self.rule.name}: {self.violation_msg}"


class RuleSet(Mapping[str, type[Rule]]):
    def __init__(self, rules: Iterable[type[Rule]] = ()) -> None:
        self._underlying = {rule.__name__.lower(): rule for rule in rules}

    def check_model(self, model: Model) -> t.List[RuleViolation]:
        violations = []

        for rule in self._underlying.values():
            violation = rule().check_model(model)

            if violation:
                violations.append(violation)

        return violations

    def __iter__(self) -> Iterator[str]:
        return iter(self._underlying)

    def __len__(self) -> int:
        return len(self._underlying)

    def __getitem__(self, rule: str | type[Rule]) -> type[Rule]:
        key = rule if isinstance(rule, str) else rule.__name__.lower()
        return self._underlying[key]

    def __op(
        self,
        op: Callable[[Set[type[Rule]], Set[type[Rule]]], Set[type[Rule]]],
        other: RuleSet,
        /,
    ) -> RuleSet:
        rules = set()
        for rule in op(set(self.values()), set(other.values())):
            rules.add(other[rule] if rule in other else self[rule])

        return RuleSet(rules)

    def union(self, *others: RuleSet) -> RuleSet:
        return reduce(lambda lhs, rhs: lhs.__op(op.or_, rhs), (self, *others))

    def intersection(self, *others: RuleSet) -> RuleSet:
        return reduce(lambda lhs, rhs: lhs.__op(op.and_, rhs), (self, *others))

    def difference(self, *others: RuleSet) -> RuleSet:
        return reduce(lambda lhs, rhs: lhs.__op(op.sub, rhs), (self, *others))
