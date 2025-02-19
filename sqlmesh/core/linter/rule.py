from __future__ import annotations

import abc

import operator as op
from collections.abc import Iterator, Set, Iterable, MutableMapping, Callable
from functools import reduce

import typing as t

from typing import overload, TypeVar


from sqlglot import exp

from sqlmesh.core.model import Model

from sqlmesh.utils.pydantic import PydanticModel

T = TypeVar("T")


class Rule(abc.ABC):
    """The base class for a rule."""

    def __init__(self, name: t.Optional[str] = None) -> None:
        self._name = name or self.__class__.__name__.lower()

    @abc.abstractmethod
    def check(self, model: Model) -> t.Optional[RuleViolation]:
        """The evaluation function that'll check for a violation of this rule."""

    @property
    @abc.abstractmethod
    def summary(self) -> str:
        """A one-line summary of what this rule checks for."""

    @property
    def name(self) -> str:
        """The name of this rule."""
        return self._name

    def __hash__(self) -> int:
        return hash(self.name)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, str):
            return self.name == other.lower()
        elif isinstance(other, Rule):
            return self.name == other.name

        return NotImplemented

    def __repr__(self) -> str:
        return self.name


class RuleViolation(PydanticModel):
    rule: Rule
    model: Model
    anchor_exprs: t.Optional[t.List[exp.Expression]] = None

    @property
    def message(self) -> str:
        return f" - {self.rule.name}: {self.rule.summary}"


class RuleSet(MutableMapping[str, type[Rule]], Set[type[Rule]]):
    def __init__(self, rules: Iterable[type[Rule]] = ()) -> None:
        self._underlying = {rule.__name__.lower(): rule for rule in rules}

    @classmethod
    def from_args(cls, *rules: type[Rule]) -> RuleSet:
        return cls(rules)

    def check(self, model: Model) -> t.List[RuleViolation]:
        violations = []

        for rule in self._underlying.values():
            violation = rule().check(model)

            if violation:
                violations.append(violation)

        return violations

    def __iter__(self) -> Iterator[str]:
        return iter(self._underlying)

    def __len__(self) -> int:
        return len(self._underlying)

    def __repr__(self) -> str:
        list_fmt = ", ".join(rule for rule in self._underlying)
        return f"[{list_fmt}]"

    def __getitem__(self, rule: str) -> type[Rule]:
        return self._underlying[rule]

    def __setitem__(self, key: str, value: type[Rule]) -> None:
        self._underlying[key] = value

    def __delitem__(self, key: str) -> None:
        del self._underlying[key]

    def __op(
        self,
        op: Callable[[Set[type[Rule]], Set[type[Rule]]], Set[type[Rule]]],
        other: RuleSet,
        /,
    ) -> RuleSet:
        result = RuleSet()

        for rule in op(set(self.values()), set(other.values())):
            rule_name = rule.__name__.lower()
            result[rule_name] = other[rule_name] if rule_name in other else self[rule_name]
        return result

    @overload
    def __or__(self, other: RuleSet) -> RuleSet: ...

    @overload
    def __or__(self, other: Set[T]) -> Set[type[Rule] | T]: ...

    def __or__(self, other: Set[T]) -> Set[type[Rule] | T]:
        if isinstance(other, RuleSet):
            return self.__op(op.or_, other)

        return super().__or__(other)

    @overload
    def __and__(self, other: RuleSet) -> RuleSet: ...

    @overload
    def __and__(self, other: Set[T]) -> Set[type[Rule] | T]: ...

    def __and__(self, other: Set[T]) -> Set[type[Rule] | T]:
        if isinstance(other, RuleSet):
            return self.__op(op.and_, other)

        return super().__and__(other)

    @overload
    def __sub__(self, other: RuleSet) -> RuleSet: ...

    @overload
    def __sub__(self, other: Set[T]) -> Set[type[Rule] | T]: ...

    def __sub__(self, other: Set[T]) -> Set[type[Rule] | T]:
        if isinstance(other, RuleSet):
            return self.__op(op.sub, other)

        return super().__sub__(other)

    @overload
    def union(self, *others: RuleSet) -> RuleSet: ...

    @overload
    def union(self, *others: Set[T]) -> Set[type[Rule] | T]: ...

    def union(self, *others: Set[T]) -> Set[type[Rule] | T]:
        return reduce(op.or_, (self, *others))

    @overload
    def intersection(self, *others: RuleSet) -> RuleSet: ...

    @overload
    def intersection(self, *others: Set[T]) -> Set[type[Rule] | T]: ...

    def intersection(self, *others: Set[T]) -> Set[type[Rule] | T]:
        return reduce(op.and_, (self, *others))

    @overload
    def difference(self, *others: RuleSet) -> RuleSet: ...

    @overload
    def difference(self, *others: Set[T]) -> Set[type[Rule] | T]: ...

    def difference(self, *others: Set[T]) -> Set[type[Rule] | T]:
        return reduce(op.sub, (self, *others))
