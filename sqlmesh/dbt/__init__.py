from sqlmesh.dbt.builtin import (
    create_builtin_filters as create_builtin_filters,
    create_builtin_globals as create_builtin_globals,
)
from sqlmesh.dbt.util import DBT_VERSION


if DBT_VERSION >= (1, 9, 0):
    from dbt.adapters.base.relation import BaseRelation, EventTimeFilter

    def _render_event_time_filtered_inclusive(
        self: BaseRelation, event_time_filter: EventTimeFilter
    ) -> str:
        """
        Returns "" if start and end are both None
        """
        filter = ""
        if event_time_filter.start and event_time_filter.end:
            filter = f"{event_time_filter.field_name} BETWEEN '{event_time_filter.start}' and '{event_time_filter.end}'"
        elif event_time_filter.start:
            filter = f"{event_time_filter.field_name} >= '{event_time_filter.start}'"
        elif event_time_filter.end:
            filter = f"{event_time_filter.field_name} <= '{event_time_filter.end}'"
        return filter

    BaseRelation._render_event_time_filtered = _render_event_time_filtered_inclusive  # type: ignore
