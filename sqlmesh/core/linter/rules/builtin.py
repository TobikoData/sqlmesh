"""Contains all the standard rules included with SQLMesh"""

from __future__ import annotations

import typing as t

from sqlglot.helper import subclasses

from sqlmesh.core.linter.rule import Rule, RuleViolation, RuleSet
from sqlmesh.core.model import Model, SqlModel


class NoSelectStar(Rule):
    """Query should not contain SELECT * on its outer most projections, even if it can be expanded."""

    def check_model(self, model: Model) -> t.Optional[RuleViolation]:
        if not isinstance(model, SqlModel):
            return None

        return self.violation() if model.query.is_star else None


class InvalidSelectStarExpansion(Rule):
    def check_model(self, model: Model) -> t.Optional[RuleViolation]:
        deps = model.violated_rules_for_query.get(InvalidSelectStarExpansion)
        if not deps:
            return None

        violation_msg = (
            f"SELECT * cannot be expanded due to missing schema(s) for model(s): {deps}. "
            "Run `sqlmesh create_external_models` and / or make sure that the model "
            f"'{model.fqn}' can be rendered at parse time."
        )

        return self.violation(violation_msg)


class AmbiguousOrInvalidColumn(Rule):
    def check_model(self, model: Model) -> t.Optional[RuleViolation]:
        sqlglot_err = model.violated_rules_for_query.get(AmbiguousOrInvalidColumn)
        if not sqlglot_err:
            return None

        violation_msg = (
            f"{sqlglot_err} for model '{model.fqn}', the column may not exist or is ambiguous."
        )

        return self.violation(violation_msg)


class NoMissingAudits(Rule):
    """Model `audits` must be configured to test data quality."""

    def check_model(self, model: Model) -> t.Optional[RuleViolation]:
        return self.violation() if not model.audits else None


BUILTIN_RULES = RuleSet(subclasses(__name__, Rule, (Rule,)))
