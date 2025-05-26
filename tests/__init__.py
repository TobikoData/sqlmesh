from sqlmesh.core import constants as c
from sqlmesh.core.analytics import disable_analytics

c.MAX_FORK_WORKERS = 1
disable_analytics()
