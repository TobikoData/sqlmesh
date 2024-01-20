import typing as t
from collections import defaultdict

ITEM = t.TypeVar("ITEM")
GROUP = t.TypeVar("GROUP")


def groupby(
    items: t.Iterable[ITEM],
    func: t.Callable[[ITEM], GROUP],
) -> defaultdict[GROUP, t.List[ITEM]]:
    grouped = defaultdict(list)
    for item in items:
        grouped[func(item)].append(item)
    return grouped
