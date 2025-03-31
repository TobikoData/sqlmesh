from __future__ import annotations

import typing as t
import unittest

from sqlmesh.core.test.result import ModelTextTestResult


class ModelTextTestRunner(unittest.TextTestRunner):
    def __init__(
        self,
        **kwargs: t.Any,
    ) -> None:
        # StringIO is used to capture the output of the tests since we'll
        # run them in parallel and we don't want to mix the output streams
        from io import StringIO

        super().__init__(
            stream=StringIO(),
            resultclass=ModelTextTestResult,
            **kwargs,
        )


def log_test_report(results: ModelTextTestResult, test_duration: float) -> None:
    # Aggregate parallel test run results
    tests_run = results.testsRun
    errors = results.errors
    failures = results.failures
    skipped = results.skipped

    is_success = not (errors or failures)

    # Compute test info
    infos = []
    if failures:
        infos.append(f"failures={len(failures)}")
    if errors:
        infos.append(f"errors={len(errors)}")
    if skipped:
        infos.append(f"skipped={skipped}")

    # Report test errors
    stream = results.stream

    stream.write("\n")

    for test_case, err in failures:
        stream.writeln(unittest.TextTestResult.separator1)
        stream.writeln(f"FAIL: {test_case}")
        stream.writeln(unittest.TextTestResult.separator2)
        stream.writeln(err)

    for error in errors:
        stream.writeln(unittest.TextTestResult.separator1)
        stream.writeln(f"ERROR: {error[1]}")
        stream.writeln(unittest.TextTestResult.separator2)

    # Test report
    stream.writeln(unittest.TextTestResult.separator2)
    stream.writeln(
        f'Ran {tests_run} {"tests" if tests_run > 1 else "test"} in {test_duration:.3f}s \n'
    )
    stream.writeln(
        f'{"OK" if is_success else "FAILED"}{" (" + ", ".join(infos) + ")" if infos else ""}'
    )
