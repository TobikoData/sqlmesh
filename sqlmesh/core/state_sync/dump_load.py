import json
import typing as t
from sqlmesh.core.state_sync import StateSync
from sqlmesh.core.snapshot import Snapshot
from sqlmesh.utils.date import now, to_tstz, to_timestamp
from sqlmesh.core.environment import Environment
from sqlmesh.utils.pydantic import _expression_encoder
from sqlmesh.core.state_sync import Versions
from sqlmesh.core.state_sync.common import StateStream
import abc
from sqlmesh.core.console import Console
from sqlmesh.core.snapshot import SnapshotNameVersion
from pathlib import Path
from sqlmesh.core.console import NoopConsole

import json_stream
from json_stream import streamable_dict, to_standard_types, streamable_list
from json_stream.writer import StreamableDict
from json_stream.base import StreamingJSONObject
from json_stream.dump import JSONStreamEncoder
from sqlmesh.utils.errors import SQLMeshError
from sqlglot import exp
from sqlmesh.core.state_sync.common import AutoRestatement
from sqlmesh.utils.pydantic import DEFAULT_ARGS as PYDANTIC_DEFAULT_ARGS, PydanticModel


class StateSerDe(abc.ABC):
    """
    Interface for producing state dumps and loading them back

    The idea is that anything produced by dump() can be loaded back by load() which is why these are part of the same object
    """

    @property
    @abc.abstractmethod
    def version(self) -> int:
        """
        Which version of the state format does this DumperLoader support
        """
        pass

    @abc.abstractmethod
    def dump(self, state_sync: StateSync, console: Console) -> StreamableDict:
        """
        Return the state as a StreamableDict that can be streamed to a file

        Args:
            state_sync: The StateSync that the user has requested to dump state from
            console: A Console instance to print progress to
        """
        pass

    @abc.abstractmethod
    def load(
        self, state_sync: StateSync, data: t.Callable[[], StreamingJSONObject], console: Console
    ) -> None:
        """
        Load the state defined by the data into the supplied StateSync. The data is in the same format as written by dump()

        Args:
            state_sync: The StateSync that the user has requested to dump state from
            data: A factory function that produces new streaming JSON reader attached to the file we are loading state from.
                This is so each section of the file can have its own reader which allows it to be read in isolation / out-of-order
                This puts less reliance on downstream consumers performing operations in a certain order
            console: A Console instance to print progress to
        """
        pass


class V1StateSerDe(StateSerDe):
    @property
    def version(self) -> int:
        return 1

    def dump(self, state_sync: StateSync, console: Console) -> StreamableDict:
        @streamable_list
        def _dump_snapshots(
            snapshot_stream: t.Iterable[Snapshot],
        ) -> t.Iterator[t.Dict[str, t.Any]]:
            console.update_state_dump_snapshots(total_snapshots=0)

            for idx, snapshot in enumerate(snapshot_stream):
                yield self._dump_pydantic_model(snapshot)
                console.update_state_dump_snapshots(snapshot=snapshot, total_snapshots=idx + 1)

        @streamable_dict
        def _dump_environments(
            environment_stream: t.Iterable[Environment],
        ) -> t.Iterator[t.Tuple[str, t.Any]]:
            console.update_state_dump_environments(total_environments=0)

            for idx, env in enumerate(environment_stream):
                yield env.name, self._dump_pydantic_model(env)
                console.update_state_dump_environments(environment=env, total_environments=idx + 1)

        @streamable_list
        def _dump_auto_restatements(
            auto_restatements: t.Iterable[AutoRestatement],
        ) -> t.Iterator[t.Dict[str, t.Any]]:
            console.update_state_dump_auto_restatements(total_auto_restatements=0)
            for idx, auto_restatement in enumerate(auto_restatements):
                as_dict = self._dump_pydantic_model(auto_restatement[0])
                as_dict["next_auto_restatement_ts"] = auto_restatement[1]
                yield as_dict
                console.update_state_dump_auto_restatements(
                    auto_restatement=auto_restatement, total_auto_restatements=idx + 1
                )

        @streamable_dict
        def _dump() -> t.Iterator[t.Tuple[str, t.Any]]:
            # the order is important for the streaming loader
            # it expects the keys to have been written in this order and will fail otherwise
            yield "timestamp", to_tstz(now())

            state_stream = state_sync.dump()

            versions = self._dump_pydantic_model(state_stream.versions)
            versions["state_dump_version"] = self.version
            yield "versions", versions
            console.update_state_dump_versions(versions)

            yield "snapshots", _dump_snapshots(state_stream.snapshots)
            console.update_state_dump_snapshots(complete=True)

            yield "environments", _dump_environments(state_stream.environments)
            console.update_state_dump_environments(complete=True)

            yield "auto_restatements", _dump_auto_restatements(state_stream.auto_restatements)
            console.update_state_dump_auto_restatements(complete=True)

        return _dump()

    def load(
        self, state_sync: StateSync, data: t.Callable[[], StreamingJSONObject], console: Console
    ) -> None:
        serde = self

        class _FileStateStream(StateStream):
            @property
            def versions(self) -> Versions:
                versions_raw = to_standard_types(data()["versions"])
                state_dump_version = versions_raw.pop("state_dump_version")

                console.update_state_load_info(state_file_version=state_dump_version)

                if state_dump_version != serde.version:
                    raise SQLMeshError(
                        f"This loader is for state dump version '{serde.version}'. The supplied state file version '{state_dump_version}' is not compatible with this loader"
                    )

                return Versions.model_validate(versions_raw)

            @property
            def snapshots(self) -> t.Iterable[Snapshot]:
                stream = data()["snapshots"]
                for idx, raw_snapshot in enumerate(stream):
                    snapshot = Snapshot.model_validate(to_standard_types(raw_snapshot))
                    yield snapshot
                    console.update_state_load_snapshots(snapshot=snapshot, total_snapshots=idx + 1)

                console.update_state_load_snapshots(complete=True)

            @property
            def environments(self) -> t.Iterable[Environment]:
                stream = data()["environments"]
                for idx, (_, raw_environment) in enumerate(stream.items()):
                    environment = Environment.model_validate(to_standard_types(raw_environment))
                    yield environment
                    console.update_state_load_environments(
                        environment=environment, total_environments=idx + 1
                    )
                console.update_state_load_environments(complete=True)

            @property
            def auto_restatements(self) -> t.Iterable[AutoRestatement]:
                stream = data()["auto_restatements"]
                for idx, raw_auto_restatement in enumerate(stream):
                    raw_auto_restatement = to_standard_types(raw_auto_restatement)
                    next_ts = to_timestamp(raw_auto_restatement.pop("next_auto_restatement_ts"))
                    auto_restatement = AutoRestatement(
                        (SnapshotNameVersion.model_validate(raw_auto_restatement), next_ts)
                    )
                    yield auto_restatement
                    console.update_state_load_auto_restatements(
                        auto_restatement=auto_restatement, total_auto_restatements=idx + 1
                    )
                console.update_state_load_auto_restatements(complete=True)

        timestamp = data()["timestamp"]
        if not isinstance(timestamp, str):
            raise ValueError(
                f"'timestamp' contains an invalid value. Expecting str, got: {timestamp}"
            )
        console.update_state_load_info(timestamp=timestamp)

        stream = _FileStateStream()

        console.update_state_load_info(versions=stream.versions)

        state_sync.load(stream)

    def _dump_pydantic_model(self, model: PydanticModel) -> t.Dict[str, t.Any]:
        dump_args: t.Dict[str, t.Any] = PYDANTIC_DEFAULT_ARGS
        return model.model_dump(mode="json", **dump_args)


class SQLMeshJSONStreamEncoder(JSONStreamEncoder):
    def default(self, obj: t.Any) -> t.Any:
        if isinstance(obj, exp.Expression):
            return _expression_encoder(obj)

        return super().default(obj)


STATE_FORMAT_VERSION_TO_LOADER_CLASS = {1: V1StateSerDe}


def _get_serde_or_raise(state_format_version: int) -> StateSerDe:
    if state_format_version in STATE_FORMAT_VERSION_TO_LOADER_CLASS:
        return STATE_FORMAT_VERSION_TO_LOADER_CLASS[state_format_version]()

    raise SQLMeshError(
        f"No serializer/deserializer implementation available for state format version: {state_format_version}"
    )


def dump_state(
    state_sync: StateSync,
    output_file: Path,
    console: t.Optional[Console] = None,
    state_format_version: int = 1,
) -> None:
    console = console or NoopConsole()
    serde = _get_serde_or_raise(state_format_version)

    try:
        console.start_state_dump_progress()
        json_stream = serde.dump(state_sync=state_sync, console=console)
        with output_file.open(mode="w", encoding="utf8") as fh:
            json.dump(json_stream, fh, indent=2, cls=SQLMeshJSONStreamEncoder)
        console.stop_state_dump_progress(success=True, output_file=output_file)
    except:
        console.stop_state_dump_progress(success=False, output_file=output_file)
        raise


def load_state(
    state_sync: StateSync,
    input_file: Path,
    console: t.Optional[Console] = None,
    confirm_fn: t.Optional[t.Callable[[], bool]] = None,
) -> None:
    console = console or NoopConsole()
    confirm_fn = confirm_fn or (lambda: True)

    # we need to peek into the file to figure out what state version we are dealing with
    with input_file.open("r", encoding="utf8") as fh:
        stream = json_stream.load(fh)
        if not isinstance(stream, StreamingJSONObject):
            raise SQLMeshError(f"Expected JSON object, got: {type(stream)}")

        try:
            versions = stream["versions"]
        except KeyError:
            raise SQLMeshError("Expecting a 'versions' key to be present")

        if not isinstance(versions, StreamingJSONObject):
            raise SQLMeshError("Expecting the 'versions' key to contain an object")

        state_dump_version = versions.get("state_dump_version")

        if state_dump_version is None:
            raise SQLMeshError("Unable to determine state dump version from the input file")

        try:
            serde = _get_serde_or_raise(int(state_dump_version))
        except ValueError:
            raise SQLMeshError(f"Unable to parse state dump version: {state_dump_version}")

    handles: t.List[t.TextIO] = []

    def _new_handle() -> StreamingJSONObject:
        handle = input_file.open("r", encoding="utf8")
        handles.append(handle)
        stream = json_stream.load(handle)
        assert isinstance(stream, StreamingJSONObject)
        return stream

    if confirm_fn():
        try:
            console.log_status_update("")
            console.start_state_load_progress()
            serde.load(state_sync, _new_handle, console)
            console.stop_state_load_progress(success=True, input_file=input_file)
        except:
            console.stop_state_load_progress(success=False, input_file=input_file)
            raise
        finally:
            for handle in handles:
                handle.close()
