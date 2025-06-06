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
        self.original_failures: t.List[t.Tuple[unittest.TestCase, ErrorType]] = []
        self.original_errors: t.List[t.Tuple[unittest.TestCase, ErrorType]] = []
        self.duration: t.Optional[float] = None

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
        exctype, value, _ = err
        self.original_failures.append((test, err))
        # Intentionally ignore the traceback to hide it from the user
        return super().addFailure(test, (exctype, value, None))  # type: ignore

    def addError(self, test: unittest.TestCase, err: ErrorType) -> None:
        """Called when the test case test signals an error.

        Args:
            test: The test case.
            err: A tuple of the form returned by sys.exc_info(), i.e., (type, value, traceback).
        """
        exctype, value, _ = err
        self.original_errors.append((test, err))
        # Intentionally ignore the traceback to hide it from the user
        return super().addError(test, (exctype, value, None))  # type: ignore

    def addSuccess(self, test: unittest.TestCase) -> None:
        """Called when the test case test succeeds.

        Args:
            test: The test case
        """
        super().addSuccess(test)
        self.successes.append(test)

    def log_test_report(self, test_duration: t.Optional[float] = None) -> None:
        """
        Log the test report following unittest's conventions.

        Args:
            test_duration: The duration of the tests.
        """
        from sqlmesh.core.console import get_console

        get_console().log_unit_test_results(self, test_duration)
