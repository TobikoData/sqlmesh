"""Contains all the standard rules included with SQLMesh"""

from __future__ import annotations

import typing as t

from sqlglot.expressions import Star
from sqlglot.helper import subclasses

from sqlmesh.core.linter.helpers import TokenPositionDetails
from sqlmesh.core.linter.rule import Rule, RuleViolation, Range
from sqlmesh.core.linter.definition import RuleSet
from sqlmesh.core.model import Model, SqlModel


class NoSelectStar(Rule):
    """Query should not contain SELECT * on its outer most projections, even if it can be expanded."""

    def check_model(self, model: Model) -> t.Optional[RuleViolation]:
        # Only applies to SQL models, as other model types do not have a query.
        if not isinstance(model, SqlModel):
            return None
        if model.query.is_star:
            violation_range = self._get_range(model)
            return self.violation(violation_range=violation_range)
        return None

    def _get_range(self, model: SqlModel) -> t.Optional[Range]:
        """Get the range of the violation if available."""
        try:
            if len(model.query.expressions) == 1 and isinstance(model.query.expressions[0], Star):
                return TokenPositionDetails.from_meta(model.query.expressions[0].meta).to_range(
                    None
                )
        except Exception:
            pass

        return None


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
        return self.violation() if not model.audits and not model.kind.is_symbolic else None


BUILTIN_RULES = RuleSet(subclasses(__name__, Rule, (Rule,)))
