"""All migrations should be named _XXXX.py, they will be executed sequentially.

If a migration alters the payload of any pydantic models, you should not actually use them because
the running model may not be able to load them. Make sure that these migration files are standalone.
"""

def migrate(state_sync):
    pass
