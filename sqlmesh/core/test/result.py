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
        return super().addFailure(test, (exctype, value, None))  # type: ignore

    def addSuccess(self, test: unittest.TestCase) -> None:
        """Called when the test case test succeeds.

        Args:
            test: The test case
        """
        super().addSuccess(test)
        self.successes.append(test)
