import os
import sys


def is_daemon_process() -> bool:
    """
    Checks if the current process is a daemon process.

    :return: True if the process is a daemon process, False otherwise.
    :rtype: bool
    """

    return not os.isatty(sys.stdout.fileno())
