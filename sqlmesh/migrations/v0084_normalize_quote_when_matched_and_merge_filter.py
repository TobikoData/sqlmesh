"""
Normalize and quote the when_matched and merge_filter properties of IncrementalByUniqueKeyKind
to match how other properties (such as time_column and partitioned_by) are handled and to
prevent un-normalized identifiers being quoted at the EngineAdapter level
"""


def migrate(state_sync, **kwargs):  # type: ignore
    pass
