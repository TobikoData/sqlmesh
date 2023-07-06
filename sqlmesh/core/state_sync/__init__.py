"""
# StateSync

State sync is how SQLMesh keeps track of environments and their states, e.g. snapshots.

# StateReader

StateReader provides a subset of the functionalities of the StateSync class. As its name
implies, it only allows for read-only operations on snapshots and environment states.

# EngineAdapterStateSync

The provided `sqlmesh.core.state_sync.EngineAdapterStateSync` leverages an existing engine
adapter to read and write state to the underlying data store.
"""
from sqlmesh.core.state_sync.base import StateReader, StateSync, Versions
from sqlmesh.core.state_sync.cache import CachingStateSync
from sqlmesh.core.state_sync.common import CommonStateSyncMixin
from sqlmesh.core.state_sync.engine_adapter import EngineAdapterStateSync
