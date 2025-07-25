"""Contains all the standard rules included with SQLMesh"""

from __future__ import annotations

import typing as t

from sqlglot.expressions import Star
from sqlglot.helper import subclasses

from sqlmesh.core.linter.helpers import TokenPositionDetails, get_range_of_model_block
from sqlmesh.core.linter.rule import Rule, RuleViolation, Range, Fix, TextEdit
from sqlmesh.core.linter.definition import RuleSet
from sqlmesh.core.model import Model, SqlModel, ExternalModel
from sqlmesh.core.model import Model, SqlModel, ExternalModel
from sqlmesh.core.linter.rules.helpers.lineage import find_external_model_ranges


class NoSelectStar(Rule):
    """Query should not contain SELECT * on its outer most projections, even if it can be expanded."""

    def check_model(self, model: Model) -> t.Optional[RuleViolation]:
        # Only applies to SQL models, as other model types do not have a query.
        if not isinstance(model, SqlModel):
            return None
        if model.query.is_star:
            violation_range = self._get_range(model)
            fixes = self._create_fixes(model, violation_range)
            return self.violation(violation_range=violation_range, fixes=fixes)
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

    def _create_fixes(
        self, model: SqlModel, violation_range: t.Optional[Range]
    ) -> t.Optional[t.List[Fix]]:
        """Create fixes for the SELECT * violation."""
        if not violation_range:
            return None
        columns = model.columns_to_types
        if not columns:
            return None
        new_text = ", ".join(columns.keys())
        return [
            Fix(
                title="Replace SELECT * with explicit column list",
                edits=[
                    TextEdit(
                        range=violation_range,
                        new_text=new_text,
                    )
                ],
            )
        ]


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
        if model.audits or model.kind.is_symbolic:
            return None
        if model._path is None or not str(model._path).endswith(".sql"):
            return self.violation()

        try:
            with open(model._path, "r", encoding="utf-8") as file:
                content = file.read()

            range = get_range_of_model_block(content, model.dialect)
            if range:
                return self.violation(violation_range=range)
            return self.violation()
        except Exception:
            return self.violation()


class NoUnregisteredExternalModels(Rule):
    """All external models must be registered in the external_models.yaml file"""

    def check_model(
        self, model: Model
    ) -> t.Optional[t.Union[RuleViolation, t.List[RuleViolation]]]:
        depends_on = model.depends_on

        # Ignore external models themselves, because either they are registered
        # if they are not, they will be caught as referenced in another model.
        if isinstance(model, ExternalModel):
            return None

        # Handle other models that are referring to them
        not_registered_external_models: t.Set[str] = set()
        for depends_on_model in depends_on:
            existing_model = self.context.get_model(depends_on_model)
            if existing_model is None:
                not_registered_external_models.add(depends_on_model)

        if not not_registered_external_models:
            return None

        path = model._path
        # For SQL models, try to do better than just raise it
        if isinstance(model, SqlModel) and path is not None and str(path).endswith(".sql"):
            external_model_ranges = self.find_external_model_ranges(
                not_registered_external_models, model
            )
            if external_model_ranges is None:
                return RuleViolation(
                    rule=self,
                    violation_msg=f"Model '{model.fqn}' depends on unregistered external models: "
                    f"{', '.join(m for m in not_registered_external_models)}. "
                    "Please register them in the external_models.yaml file.",
                )

            outs: t.List[RuleViolation] = []
            for external_model in not_registered_external_models:
                external_model_range = external_model_ranges.get(external_model)
                if external_model_range:
                    outs.extend(
                        RuleViolation(
                            rule=self,
                            violation_msg=f"Model '{model.fqn}' depends on unregistered external model: "
                            f"{external_model}. Please register it in the external_models.yaml file.",
                            violation_range=target,
                        )
                        for target in external_model_range
                    )
                else:
                    outs.append(
                        RuleViolation(
                            rule=self,
                            violation_msg=f"Model '{model.fqn}' depends on unregistered external model: "
                            f"{external_model}. Please register it in the external_models.yaml file.",
                        )
                    )

            return outs

        return RuleViolation(
            rule=self,
            violation_msg=f"Model '{model.name}' depends on unregistered external models: "
            f"{', '.join(m for m in not_registered_external_models)}. "
            "Please register them in the external_models.yaml file.",
        )

    def find_external_model_ranges(
        self, external_models_not_registered: t.Set[str], model: SqlModel
    ) -> t.Optional[t.Dict[str, t.List[Range]]]:
        """Returns a map of external model names to their ranges found in the query.

        It returns a dictionary of fqn to a list of ranges where the external model
        """
        return find_external_model_ranges(self.context, external_models_not_registered, model)


BUILTIN_RULES = RuleSet(subclasses(__name__, Rule, (Rule,)))
