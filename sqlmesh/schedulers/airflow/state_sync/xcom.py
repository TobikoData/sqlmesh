import json
import logging
import typing as t
from functools import reduce
from operator import or_

from airflow.models import DagRun, XCom
from sqlalchemy.orm import Session

from sqlmesh.core.environment import Environment
from sqlmesh.core.snapshot import Snapshot, SnapshotId, SnapshotIdLike, SnapshotInfoLike
from sqlmesh.core.state_sync import CommonStateSyncMixin, StateSync
from sqlmesh.schedulers.airflow import common, util
from sqlmesh.utils import unique

logger = logging.getLogger(__name__)


class XComStateSync(CommonStateSyncMixin, StateSync):
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
            {common.snapshot_version_xcom_key(s.name, s.version) for s in snapshots}
        )

        for snapshot in snapshots:
            logger.info("Storing model snapshot %s", snapshot.snapshot_id)
            # TODO: consider inserting records in bulk using the DB session directly
            # instead of the TaskInstance API.
            self._xcom_set(
                key=common.snapshot_xcom_key(snapshot),
                value=snapshot.json(),
            )

            version_key = common.snapshot_version_xcom_key(
                snapshot.name, snapshot.version
            )
            if version_key in snapshot_version_index:
                snapshot_version_index[version_key].append(snapshot.fingerprint)
            else:
                snapshot_version_index[version_key] = [snapshot.fingerprint]

        for key, value in snapshot_version_index.items():
            logger.info("Storing the snapshot version index '%s'", key)
            self._xcom_set(
                key=key,
                value=unique(value),
            )

    def delete_snapshots(self, snapshot_ids: t.Iterable[SnapshotIdLike]) -> None:
        for snapshot_id in snapshot_ids:
            logger.info("Removing snapshot %s", snapshot_id.snapshot_id)
        self._delete_xcom_records(
            target_keys={common.snapshot_xcom_key(s.snapshot_id) for s in snapshot_ids}
        )

    def get_environments(self) -> t.List[Environment]:
        return [
            Environment.parse_raw(value)
            for value in self._get_xcom_records(
                key_prefix=common.ENV_KEY_PREFIX
            ).values()
        ]

    def snapshots_exist(
        self, snapshot_ids: t.Iterable[SnapshotIdLike]
    ) -> t.Set[SnapshotId]:
        target_keys = {common.snapshot_xcom_key(s) for s in snapshot_ids}
        raw_snapshots = self._get_snapshots_raw(target_keys=target_keys)
        return {
            SnapshotId(name=s["name"], fingerprint=s["fingerprint"])
            for s in raw_snapshots.values()
        }

    def get_all_snapshots_raw(self) -> t.List[t.Dict]:
        return list(self._get_snapshots_raw().values())

    def get_all_snapshots(self) -> t.Dict[SnapshotId, Snapshot]:
        return self._get_snapshots()

    def _get_snapshots_with_same_version(
        self,
        snapshots: t.Iterable[SnapshotInfoLike],
        lock_for_update: bool = False,
    ) -> t.List[Snapshot]:
        snapshots_dict = self._get_snapshots(
            snapshot_ids=(snapshot.snapshot_id for snapshot in snapshots),
            lock_for_update=lock_for_update,
        )
        version_index = self._get_snapshot_versions(
            target_keys={
                common.snapshot_version_xcom_key(s.name, s.version)
                for s in snapshots_dict.values()
            }
        )

        all_snapshot_ids = reduce(
            or_,
            [
                {
                    SnapshotId(
                        name=common.name_from_snapshot_version_xcom_key(key),
                        fingerprint=f,
                    )
                    for f in fingerprints
                }
                for key, fingerprints in version_index.items()
            ],
        )
        missing_keys = all_snapshot_ids - snapshots_dict.keys()
        missing_snapshots = self._get_snapshots(
            snapshot_ids=missing_keys,
            lock_for_update=lock_for_update,
        )

        return list({**snapshots_dict, **missing_snapshots}.values())

    def _get_environment(
        self, environment: str, lock_for_update: bool = False
    ) -> t.Optional[Environment]:
        model_env_state_xcom_value = self._get_one_xcom_record(
            common.environment_xcom_key(environment), lock_for_update=lock_for_update
        )

        if model_env_state_xcom_value:
            return Environment.parse_raw(model_env_state_xcom_value)
        return None

    def _update_environment(self, environment: Environment) -> None:
        self._xcom_set(
            common.environment_xcom_key(environment.name),
            environment.dict(),
        )

    def _update_snapshot(self, snapshot: Snapshot) -> None:
        self._xcom_set(
            key=common.snapshot_xcom_key(snapshot),
            value=snapshot.json(),
        )

    def _xcom_set(self, key: str, value: t.Any) -> None:
        if not isinstance(value, str):
            value = json.dumps(value)
        serialized_value = _serialize_xcom_value(value)

        existing_xcom = (
            self._session.query(XCom)
            .filter_by(
                dag_run_id=self._get_receiver_dag_run_id(),
                task_id=common.SQLMESH_XCOM_TASK_ID,
                map_index=-1,
                key=key,
            )
            .one_or_none()
        )
        if existing_xcom is not None:
            existing_xcom.value = serialized_value
        else:
            new_xcom = XCom(  # type: ignore
                dag_run_id=self._get_receiver_dag_run_id(),
                key=key,
                value=serialized_value,
                task_id=common.SQLMESH_XCOM_TASK_ID,
                dag_id=common.SQLMESH_XCOM_DAG_ID,
                run_id=common.INIT_RUN_ID,
            )
            self._session.add(new_xcom)
        self._session.flush()

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
            target_keys = {common.snapshot_xcom_key(s) for s in snapshot_ids}
        raw_snapshots = self._get_snapshots_raw(
            target_keys=target_keys, lock_for_update=lock_for_update
        )
        return {
            SnapshotId(
                name=s["name"], fingerprint=s["fingerprint"]
            ): Snapshot.parse_obj(s)
            for s in raw_snapshots.values()
        }

    def _get_snapshots_raw(
        self, target_keys: t.Optional[t.Set[str]] = None, lock_for_update: bool = False
    ) -> t.Dict[str, t.Dict]:
        """Fetches snapshot payloads.

        Args:
            target_keys: The list of target XCom keys.
                If not specified all keys will be fetched.
            lock_for_update: Indicates whether fetched records should be locked
                to prevent race conditions while updating them concurrently.

        Returns:
            A dictionary of XCom keys to snapshot payloads associated with them.
        """
        return {
            xcom_key: json.loads(xcom_value)
            for xcom_key, xcom_value in self._get_xcom_records(
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
            target_keys: The list of target XCom keys.
            lock_for_update: Indicates whether fetched records should be locked
                to prevent race conditions while updating them concurrently.

        Returns:
            A dictionary of XCom keys to version indexes associated with them.
        """
        return {
            xcom_key: json.loads(xcom_value)
            for xcom_key, xcom_value in self._get_xcom_records(
                target_keys=target_keys,
                key_prefix=common.SNAPSHOT_VERSION_KEY_PREFIX,
                lock_for_update=lock_for_update,
            ).items()
        }  # type: ignore

    def _get_one_xcom_record(
        self, target_key: str, lock_for_update: bool = False
    ) -> t.Optional[str]:
        """Fetches a single raw XCom value.

        Args:
            target_key: The target XCom key.
            lock_for_update: Indicates whether the fetched record should be locked
                to prevent race conditions while updating it concurrently.

        Returns:
            The XCom value associated with the target key.
        """
        query = self._session.query(XCom.value).filter(
            XCom.dag_id == common.SQLMESH_XCOM_DAG_ID,
            XCom.key == target_key,
        )
        if lock_for_update:
            query = query.with_for_update()
        result = query.one_or_none()
        return _deserialize_xcom_value(result[0]) if result is not None else None

    def _get_xcom_records(
        self,
        target_keys: t.Optional[t.Set[str]] = None,
        key_prefix: t.Optional[str] = None,
        key_suffix: t.Optional[str] = None,
        lock_for_update: bool = False,
    ) -> t.Dict[str, str]:
        """Fetches raw XCom values.

        Args:
            target_keys: The list of target XCom keys.
                If not specified all keys will be fetched.
            key_prefix: The optional prefix to filter XCom keys by.
            key_suffix: The optional suffix to filter XCom keys by.
            lock_for_update: Indicates whether fetched records should be locked
                to prevent race conditions while updating them concurrently.

        Returns:
            A dictionary of XCom keys to values associated with them.
        """
        query = self._session.query(XCom.key, XCom.value).filter(
            XCom.dag_id == common.SQLMESH_XCOM_DAG_ID
        )
        if target_keys is not None:
            query = query.filter(XCom.key.in_(target_keys))
        else:
            if key_prefix is not None:
                query = query.filter(XCom.key.like(f"{key_prefix}%"))
            if key_suffix is not None:
                query = query.filter(XCom.key.like(f"%{key_suffix}"))
        if lock_for_update:
            query = query.with_for_update()
        xcoms = query.all()
        return {x[0]: _deserialize_xcom_value(x[1]) for x in xcoms}

    def _delete_xcom_records(self, target_keys: t.Set[str]) -> None:
        util.delete_xcoms(
            common.SQLMESH_XCOM_DAG_ID, target_keys, session=self._session
        )

    def _get_receiver_dag_run_id(self) -> int:
        if self._receiver_dag_run_id is None:
            self._receiver_dag_run_id = (
                self._session.query(DagRun.id)  # type: ignore
                .filter_by(dag_id=common.SQLMESH_XCOM_DAG_ID, run_id=common.INIT_RUN_ID)
                .scalar()
            )
        return self._receiver_dag_run_id


def _serialize_xcom_value(value: str) -> bytes:
    # Serialize the input string as JSON again to prevent Airflow
    # from producing a malformed JSON string when it returns the XCom
    # record via REST API.
    return json.dumps(value).encode("UTF-8")


def _deserialize_xcom_value(value: bytes) -> str:
    return json.loads(value.decode("UTF-8"))
