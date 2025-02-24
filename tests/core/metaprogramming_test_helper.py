# This module is conditionally imported to demonstrate that we can
# serialize `match` statements that are available only for > v3.9.
# If we included this code as-is in test_metaprogramming.py, the
# test harness would crash when using Python 3.9 in CI


def match_expression():
    match 5:
        case 5:
            return 1
    return 0
