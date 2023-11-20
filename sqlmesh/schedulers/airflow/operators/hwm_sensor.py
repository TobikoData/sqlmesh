from __future__ import annotations

import logging
import typing as t
from datetime import datetime

from airflow.models import DagRun
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context

from sqlmesh.core.snapshot import Snapshot, SnapshotTableInfo
from sqlmesh.schedulers.airflow import util
from sqlmesh.utils.date import TimeLike, now, to_datetime

if t.TYPE_CHECKING:
    from airflow.sensors.base import PokeReturnValue

logger = logging.getLogger(__name__)


class HighWaterMarkSensor(BaseSensorOperator):
    def __init__(
        self,
        target_snapshot_info: SnapshotTableInfo,
        this_snapshot: Snapshot,
        poke_interval: float = 60.0,
        timeout: float = 7.0 * 24.0 * 60.0 * 60.0,  # 7 days
        mode: str = "reschedule",
        **kwargs: t.Any,
    ) -> None:
        super().__init__(
            poke_interval=poke_interval,
            timeout=timeout,
            mode=mode,
            **kwargs,
        )
        self.target_snapshot_info = target_snapshot_info
        self.this_snapshot = this_snapshot

    def poke(self, context: Context) -> bool:
        dag_run = context["dag_run"]

        with util.scoped_state_sync() as state_sync:
            target_snapshot = state_sync.get_snapshots([self.target_snapshot_info])[
                self.target_snapshot_info.snapshot_id
            ]
        if target_snapshot.intervals:
            current_high_water_mark = to_datetime(target_snapshot.intervals[-1][1])
        else:
            current_high_water_mark = None

        target_high_water_mark = self._compute_target_high_water_mark(dag_run, target_snapshot)

        logger.info(
            "The current high water mark for snapshot %s is '%s' (target is '%s')",
            self.target_snapshot_info.snapshot_id,
            current_high_water_mark,
            target_high_water_mark,
        )
        if current_high_water_mark is not None:
            return current_high_water_mark >= target_high_water_mark
        return False

    def _compute_target_high_water_mark(
        self, dag_run: DagRun, target_snapshot: Snapshot
    ) -> datetime:
        target_date = to_datetime(dag_run.data_interval_end)
        target_prev = to_datetime(target_snapshot.node.interval_unit.cron_floor(target_date))
        this_prev = to_datetime(self.this_snapshot.node.interval_unit.cron_floor(target_date))
        return min(target_prev, this_prev)


class HighWaterMarkExternalSensor(BaseSensorOperator):
    def __init__(
        self,
        snapshot: Snapshot,
        external_table_sensor_factory: t.Callable[[t.Dict[str, t.Any]], BaseSensorOperator],
        poke_interval: float = 60.0,
        timeout: float = 7.0 * 24.0 * 60.0 * 60.0,  # 7 days
        mode: str = "reschedule",
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
        **kwargs: t.Any,
    ):
        super().__init__(
            poke_interval=poke_interval,
            timeout=timeout,
            mode=mode,
            **kwargs,
        )
        self.snapshot = snapshot
        self.external_table_sensor_factory = external_table_sensor_factory
        self.start = start
        self.end = end

    def poke(self, context: Context) -> t.Union[bool, PokeReturnValue]:
        interval_unit = self.snapshot.node.interval_unit
        dag_run = context["dag_run"]
        signals = self.snapshot.model.render_signals(
            start=interval_unit.cron_floor(self.start or dag_run.data_interval_start),
            end=interval_unit.cron_floor(self.end or dag_run.data_interval_end),
            execution_time=now(minute_floor=False),
        )
        delegates = [self.external_table_sensor_factory(signal) for signal in signals]
        return all(d.poke(context) for d in delegates)
