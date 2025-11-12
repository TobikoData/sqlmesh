from __future__ import annotations

import inspect
import typing as t
from unittest.mock import MagicMock

import pytest
from sqlmesh.core.model import Model
from sqlmesh.core.linter.rule import Rule, RuleViolation


class TestRule(Rule):
    """A test rule for testing the get_definition_location method."""

    __test__ = False  # prevent pytest warning since this isnt a class containing tests

    def check_model(self, model: Model) -> t.Optional[RuleViolation]:
        """The evaluation function that'll check for a violation of this rule."""
        return None


def test_get_definition_location():
    """Test the get_definition_location method returns correct file and line information."""
    # Create a mock context
    mock_context = MagicMock()
    rule = TestRule(mock_context)

    # Get the expected location using the inspect module
    expected_file = inspect.getfile(TestRule)
    expected_source, expected_start_line = inspect.getsourcelines(TestRule)
    expected_end_line = expected_start_line + len(expected_source) - 1

    # Get the location using the Rule method
    location = rule.get_definition_location()

    # Assert the file path matches
    assert location.file_path == expected_file

    # Assert the line numbers match
    assert location.start_line == expected_start_line

    # Test the fallback case for a class without source
    with pytest.MonkeyPatch.context() as mp:
        # Mock inspect.getsourcelines to raise an exception
        def mock_getsourcelines(*args, **kwargs):
            raise IOError("Mock error")

        mp.setattr(inspect, "getsourcelines", mock_getsourcelines)

        # Get the location with the mocked function
        fallback_location = rule.get_definition_location()

        # It should still have the file path
        assert fallback_location.file_path == expected_file

        # But not the line numbers
        assert fallback_location.start_line is None
