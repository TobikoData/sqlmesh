from __future__ import annotations

import operator as op
import typing as t
from collections.abc import Iterator, Iterable, Set, Mapping, Callable
from functools import reduce

from sqlmesh.core.config.linter import LinterConfig
from sqlmesh.core.console import LinterConsole, get_console
from sqlmesh.core.linter.rule import Rule, RuleViolation, Range, Fix
from sqlmesh.core.model import Model
from sqlmesh.utils.errors import raise_config_error

if t.TYPE_CHECKING:
    from sqlmesh.core.context import GenericContext


def select_rules(all_rules: RuleSet, rule_names: t.Set[str]) -> RuleSet:
    if "all" in rule_names:
        return all_rules

    rules = set()
    for rule_name in rule_names:
        if rule_name not in all_rules:
            raise_config_error(f"Rule {rule_name} could not be found")

        rules.add(all_rules[rule_name])

    return RuleSet(rules)


class Linter:
    def __init__(
        self, enabled: bool, all_rules: RuleSet, rules: RuleSet, warn_rules: RuleSet
    ) -> None:
        self.enabled = enabled
        self.all_rules = all_rules
        self.rules = rules
        self.warn_rules = warn_rules

        if overlapping := rules.intersection(warn_rules):
            overlapping_rules = ", ".join(rule for rule in overlapping)
            raise_config_error(
                f"Rules cannot simultaneously warn and raise an error: [{overlapping_rules}]"
            )

    @classmethod
    def from_rules(cls, all_rules: RuleSet, config: LinterConfig) -> Linter:
        ignored_rules = select_rules(all_rules, config.ignored_rules)
        included_rules = all_rules.difference(ignored_rules)

        rules = select_rules(included_rules, config.rules)
        warn_rules = select_rules(included_rules, config.warn_rules)

        return Linter(config.enabled, all_rules, rules, warn_rules)

    def lint_model(
        self, model: Model, context: GenericContext, console: LinterConsole = get_console()
    ) -> t.Tuple[bool, t.List[AnnotatedRuleViolation]]:
        if not self.enabled:
            return False, []

        ignored_rules = select_rules(self.all_rules, model.ignored_rules)

        rules = self.rules.difference(ignored_rules)
        warn_rules = self.warn_rules.difference(ignored_rules)

        error_violations = rules.check_model(model, context)
        warn_violations = warn_rules.check_model(model, context)

        all_violations: t.List[AnnotatedRuleViolation] = [
            AnnotatedRuleViolation(
                rule=violation.rule,
                violation_msg=violation.violation_msg,
                model=model,
                violation_type="error",
                violation_range=violation.violation_range,
                fixes=violation.fixes,
            )
            for violation in error_violations
        ] + [
            AnnotatedRuleViolation(
                rule=violation.rule,
                violation_msg=violation.violation_msg,
                model=model,
                violation_type="warning",
                violation_range=violation.violation_range,
                fixes=violation.fixes,
            )
            for violation in warn_violations
        ]

        if warn_violations:
            console.show_linter_violations(warn_violations, model)
        if error_violations:
            console.show_linter_violations(error_violations, model, is_error=True)
            return True, all_violations

        return False, all_violations


class RuleSet(Mapping[str, type[Rule]]):
    def __init__(self, rules: Iterable[type[Rule]] = ()) -> None:
        self._underlying = {rule.name: rule for rule in rules}

    def check_model(self, model: Model, context: GenericContext) -> t.List[RuleViolation]:
        violations = []

        for rule in self._underlying.values():
            violation = rule(context).check_model(model)
            if isinstance(violation, RuleViolation):
                violation = [violation]
            if violation:
                violations.extend(violation)

        return violations

    def __iter__(self) -> Iterator[str]:
        return iter(self._underlying)

    def __len__(self) -> int:
        return len(self._underlying)

    def __getitem__(self, rule: str | type[Rule]) -> type[Rule]:
        key = rule if isinstance(rule, str) else rule.name
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


class AnnotatedRuleViolation(RuleViolation):
    def __init__(
        self,
        rule: Rule,
        violation_msg: str,
        model: Model,
        violation_type: t.Literal["error", "warning"],
        violation_range: t.Optional[Range] = None,
        fixes: t.Optional[t.List[Fix]] = None,
    ) -> None:
        super().__init__(rule, violation_msg, violation_range, fixes)
        self.model = model
        self.violation_type = violation_type
