# Linter

Linting enables you to validate the model definition, ensuring it adheres to best practices. When a new [plan](plans.md) is created, each model is checked against a set of built-in and user-defined rules to verify that its attributes comply with the defined standards; This improves code quality, consistency, and helps to detect issues early in the development cycle.

For more information regarding linter configuration visit the relevant [guide here](../guides/configuration.md).

# Rules

Each rule is responsible for detecting a specific issue or pattern within a model. Rules are defined as classes that implement the logic for validation by subclassing `Rule` (redacted form):

```Python3
class Rule:
    """The base class for a rule."""

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

```

Thus, each `Rule` can be broken down to it's vital components:
- The name (or code) of the rule is it's class name in lowercase.
- The core logic is implemented in `Rule::check_model(...)` which can examine any `Model` attribute.
- If an issue is detected, a `RuleViolation` object should be returned with the proper context. This can be created manually or through the `Rule::violation()` helper.
- A short explanation of the Rule's workings should be added in the form of a class docstring or by subclassing `Rule::summary`.



## Built-in
SQLMesh comes with a set of predefined rules which check for potential SQL errors or enforce stylistic opinions. An example of the latter the `NoSelectStar` rule which prohibits users from writing `SELECT *` in the outer-most select:


```Python
class NoSelectStar(Rule):
    """Query should not contain SELECT * on its outer most projections, even if it can be expanded."""

    def check_model(self, model: Model) -> t.Optional[RuleViolation]:
        if not isinstance(model, SqlModel):
            return None

        return self.violation() if model.query.is_star else None
```


The list of all built-in rules is the following:


| Name                                 | Explanation
|--------------------------------------|--------------------------------------------------------------------------------------------------------|
| ambiguousorinvalidcolumns            | The optimizer was unable to trace or found duplicate columns                                           |
| invalidselectstarexpansion           | The optimizer was unable to expand the top-level `SELECT *`                                            |
| noselectstar                         | The query's top-level projection should not be `SELECT *`, even if it can be expanded by the optimizer |


## User defined rules
Users can implement their own custom rules in a similar fashion. For that matter, SQLMesh will attempt to load any subclass of `Rule` under the `linter/` directory.

For instance, if an organization wanted to ensure all models are owned by an engineer, one solution would be to implement the following rule:

```Python
# linter/user.py

import typing as t

from sqlmesh.core.linter.rule import Rule, RuleViolation, RuleSet
from sqlmesh.core.model import Model

class NoMissingOwner(Rule):
    """Model owner always should be specified."""

    def check_model(self, model: Model) -> t.Optional[RuleViolation]:
        return self.violation() if not model.owner else None

```
