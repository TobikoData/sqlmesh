import json
import logging
import typing as t
from functools import reduce
from operator import or_

from airflow.models import Variable
from sqlalchemy.orm import Session

from sqlmesh.core.environment import Environment
from sqlmesh.core.snapshot import (
    Snapshot,
    SnapshotId,
    SnapshotIdLike,
    SnapshotNameVersionLike,
)
from sqlmesh.core.state_sync import CommonStateSyncMixin, StateSync
from sqlmesh.schedulers.airflow import common, util
from sqlmesh.utils import unique

logger = logging.getLogger(__name__)


class VariableStateSync(CommonStateSyncMixin, StateSync):
    def __init__(
        self,
        session: Session,
    ):
        self._session = session
        self._receiver_dag_run_id: t.Optional[int] = None

    def init_schema(self) -> None:
        """Nothing to initialize."""

    def push_snapshots(self, snapshots: t.Iterable[Snapshot]) -> None:
        snapshot_version_index = self._get_snapshot_versions(
            {common.snapshot_version_key(s.name, s.version) for s in snapshots}
        )

        for snapshot in snapshots:
            logger.info("Storing model snapshot %s", snapshot.snapshot_id)
            # TODO: consider inserting records in bulk using the DB session directly
            # instead of the TaskInstance API.
            self._set_record(
                key=common.snapshot_key(snapshot),
                value=snapshot.json(),
            )

            version_key = common.snapshot_version_key(snapshot.name, snapshot.version)
            if version_key in snapshot_version_index:
                snapshot_version_index[version_key].append(snapshot.identifier)
            else:
                snapshot_version_index[version_key] = [snapshot.identifier]

        for key, value in snapshot_version_index.items():
            logger.info("Storing the snapshot version index '%s'", key)
            self._set_record(
                key=key,
                value=unique(value),
            )

    def delete_snapshots(self, snapshot_ids: t.Iterable[SnapshotIdLike]) -> None:
        for snapshot_id in snapshot_ids:
            logger.info("Removing snapshot %s", snapshot_id.snapshot_id)
        self._delete_records(
            target_keys={common.snapshot_key(s.snapshot_id) for s in snapshot_ids}
        )

    def get_environments(self) -> t.List[Environment]:
        return [
            Environment.parse_raw(value)
            for value in self._get_records(key_prefix=common.ENV_KEY_PREFIX).values()
        ]

    def snapshots_exist(
        self, snapshot_ids: t.Iterable[SnapshotIdLike]
    ) -> t.Set[SnapshotId]:
        target_keys = {common.snapshot_key(s): s for s in snapshot_ids}
        raw_snapshots = self._get_snapshots_raw(target_keys=set(target_keys))
        return {
            snapshot_id.snapshot_id
            for key, snapshot_id in target_keys.items()
            if key in raw_snapshots
        }

    def get_all_snapshots_raw(self) -> t.List[t.Dict]:
        return list(self._get_snapshots_raw().values())

    def get_all_snapshots(self) -> t.Dict[SnapshotId, Snapshot]:
        return self._get_snapshots()

    def _get_snapshots_with_same_version(
        self,
        snapshots: t.Iterable[SnapshotNameVersionLike],
        lock_for_update: bool = False,
    ) -> t.List[Snapshot]:
        version_index = self._get_snapshot_versions(
            target_keys={
                common.snapshot_version_key(s.name, s.version) for s in snapshots
            }
        )

        snapshot_ids = reduce(
            or_,
            [
                {
                    SnapshotId(
                        name=common.name_from_snapshot_version_key(key),
                        identifier=i,
                    )
                    for i in identifiers
                }
                for key, identifiers in version_index.items()
            ],
        )

        all_snapshots = self._get_snapshots(
            snapshot_ids=snapshot_ids,
            lock_for_update=lock_for_update,
        )

        return list(all_snapshots.values())

    def _get_environment(
        self, environment: str, lock_for_update: bool = False
    ) -> t.Optional[Environment]:
        model_env_state_value = self._get_one_record(
            common.environment_key(environment), lock_for_update=lock_for_update
        )

        if model_env_state_value:
            return Environment.parse_raw(model_env_state_value)
        return None

    def _update_environment(self, environment: Environment) -> None:
        self._set_record(
            common.environment_key(environment.name),
            environment.dict(),
        )

    def _update_snapshot(self, snapshot: Snapshot) -> None:
        self._set_record(
            key=common.snapshot_key(snapshot),
            value=snapshot.json(),
        )

    def _get_snapshots(
        self,
        snapshot_ids: t.Optional[t.Iterable[SnapshotIdLike]] = None,
        lock_for_update: bool = False,
    ) -> t.Dict[SnapshotId, Snapshot]:
        """Fetches parsed snapshots.

        Args:
            snapshot_ids: The list of target snapshot IDs.
                If not specified all snapshots will be fetched.
            lock_for_update: Indicates whether fetched records should be locked
                to prevent race conditions while updating them concurrently.

        Returns:
            A dictionary of snapshot IDs to snapshots.
        """
        target_keys = None
        if snapshot_ids is not None:
            target_keys = {common.snapshot_key(s) for s in snapshot_ids}
        raw_snapshots = self._get_snapshots_raw(
            target_keys=target_keys, lock_for_update=lock_for_update
        )
        result = {}
        for raw_snapshot in raw_snapshots.values():
            snapshot = Snapshot.parse_obj(raw_snapshot)
            result[snapshot.snapshot_id] = snapshot
        return result

    def _get_snapshots_raw(
        self, target_keys: t.Optional[t.Set[str]] = None, lock_for_update: bool = False
    ) -> t.Dict[str, t.Dict]:
        """Fetches snapshot payloads.

        Args:
            target_keys: The list of target Variable keys.
                If not specified all keys will be fetched.
            lock_for_update: Indicates whether fetched records should be locked
                to prevent race conditions while updating them concurrently.

        Returns:
            A dictionary of Variable keys to snapshot payloads associated with them.
        """
        return {
            key: json.loads(value)
            for key, value in self._get_records(
                target_keys=target_keys,
                key_prefix=common.SNAPSHOT_PAYLOAD_KEY_PREFIX,
                lock_for_update=lock_for_update,
            ).items()
        }  # type: ignore

    def _get_snapshot_versions(
        self, target_keys: t.Set[str], lock_for_update: bool = False
    ) -> t.Dict[str, t.List[str]]:
        """Fetches snapshot version indexes.

        Args:
            target_keys: The list of target Variable keys.
            lock_for_update: Indicates whether fetched records should be locked
                to prevent race conditions while updating them concurrently.

        Returns:
            A dictionary of Variable keys to version indexes associated with them.
        """
        return {
            key: json.loads(value)
            for key, value in self._get_records(
                target_keys=target_keys,
                key_prefix=common.SNAPSHOT_VERSION_KEY_PREFIX,
                lock_for_update=lock_for_update,
            ).items()
        }  # type: ignore

    def _get_one_record(
        self, target_key: str, lock_for_update: bool = False
    ) -> t.Optional[str]:
        """Fetches a single raw Variable value.

        Args:
            target_key: The target Variable key.
            lock_for_update: Indicates whether the fetched record should be locked
                to prevent race conditions while updating it concurrently.

        Returns:
            The Variable value associated with the target key.
        """
        query = self._session.query(Variable).filter(Variable.key == target_key)
        if lock_for_update:
            query = query.with_for_update()
        result = query.one_or_none()
        return result.val if result is not None else None

    def _get_records(
        self,
        target_keys: t.Optional[t.Set[str]] = None,
        key_prefix: t.Optional[str] = None,
        key_suffix: t.Optional[str] = None,
        lock_for_update: bool = False,
    ) -> t.Dict[str, str]:
        """Fetches raw Variable values.

        Args:
            target_keys: The list of target Variable keys.
                If not specified all keys will be fetched.
            key_prefix: The optional prefix to filter Variable keys by.
            key_suffix: The optional suffix to filter Variable keys by.
            lock_for_update: Indicates whether fetched records should be locked
                to prevent race conditions while updating them concurrently.

        Returns:
            A dictionary of Variable keys to values associated with them.
        """
        query = self._session.query(Variable)
        if target_keys is not None:
            query = query.filter(Variable.key.in_(target_keys))
        else:
            if key_prefix is not None:
                query = query.filter(Variable.key.like(f"{key_prefix}%"))
            if key_suffix is not None:
                query = query.filter(Variable.key.like(f"%{key_suffix}"))
        if lock_for_update:
            query = query.with_for_update()
        variables = query.all()
        return {v.key: v.val for v in variables}

    def _set_record(self, key: str, value: t.Any) -> None:
        if not isinstance(value, str):
            value = json.dumps(value)

        self._delete_records({key})
        new_variable = Variable(
            key=key,
            val=value,
            description=None,
        )
        self._session.add(new_variable)
        self._session.flush()

    def _delete_records(self, target_keys: t.Set[str]) -> None:
        util.delete_variables(target_keys, session=self._session)
