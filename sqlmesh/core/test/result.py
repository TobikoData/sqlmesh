from __future__ import annotations

import types
import typing as t
import unittest

if t.TYPE_CHECKING:
    ErrorType = t.Union[
        t.Tuple[type[BaseException], BaseException, types.TracebackType],
        t.Tuple[None, None, None],
    ]


class ModelTextTestResult(unittest.TextTestResult):
    successes: t.List[unittest.TestCase]

    def __init__(self, *args: t.Any, **kwargs: t.Any):
        super().__init__(*args, **kwargs)
        self.successes = []

    def addSubTest(
        self,
        test: unittest.TestCase,
        subtest: unittest.TestCase,
        err: t.Optional[ErrorType],
    ) -> None:
        """Called at the end of a subtest.

        The traceback is suppressed because it is redundant and not useful.

        Args:
            test: The test case.
            subtest: The subtest instance.
            err: A tuple of the form returned by sys.exc_info(), i.e., (type, value, traceback).
        """
        if err:
            exctype, value, tb = err
            err = (exctype, value, None)  # type: ignore

        super().addSubTest(test, subtest, err)

    def addFailure(self, test: unittest.TestCase, err: ErrorType) -> None:
        """Called when the test case test signals a failure.

        The traceback is suppressed because it is redundant and not useful.

        Args:
            test: The test case.
            err: A tuple of the form returned by sys.exc_info(), i.e., (type, value, traceback).
        """
        exctype, value, tb = err
        self.original_err = (test, err)
        return super().addFailure(test, (exctype, value, None))  # type: ignore

    def addError(self, test: unittest.TestCase, err: ErrorType) -> None:
        """Called when the test case test signals an error.

        Args:
            test: The test case.
            err: A tuple of the form returned by sys.exc_info(), i.e., (type, value, traceback).
        """
        self.original_err = (test, err)
        return super().addError(test, err)  # type: ignore

    def addSuccess(self, test: unittest.TestCase) -> None:
        """Called when the test case test succeeds.

        Args:
            test: The test case
        """
        super().addSuccess(test)
        self.successes.append(test)

    def log_test_report(self, test_duration: float) -> None:
        """
        Log the test report following unittest's conventions.

        Args:
            test_duration: The duration of the tests.
        """
        tests_run = self.testsRun
        errors = self.errors
        failures = self.failures
        skipped = self.skipped

        is_success = not (errors or failures)

        infos = []
        if failures:
            infos.append(f"failures={len(failures)}")
        if errors:
            infos.append(f"errors={len(errors)}")
        if skipped:
            infos.append(f"skipped={skipped}")

        stream = self.stream

        stream.write("\n")

        for test_case, failure in failures:
            stream.writeln(unittest.TextTestResult.separator1)
            stream.writeln(f"FAIL: {test_case}")
            stream.writeln(f"{test_case.shortDescription()}")
            stream.writeln(unittest.TextTestResult.separator2)
            stream.writeln(failure)

        for _, error in errors:
            stream.writeln(unittest.TextTestResult.separator1)
            stream.writeln(f"ERROR: {error}")
            stream.writeln(unittest.TextTestResult.separator2)

        # Output final report
        stream.writeln(unittest.TextTestResult.separator2)
        stream.writeln(
            f'Ran {tests_run} {"tests" if tests_run > 1 else "test"} in {test_duration:.3f}s \n'
        )
        stream.writeln(
            f'{"OK" if is_success else "FAILED"}{" (" + ", ".join(infos) + ")" if infos else ""}'
        )
