from __future__ import annotations

from sqlmesh.utils import UniqueKeyDict, registry_decorator


class hook(registry_decorator):
    """Specifies a function is a hook and registers it the global hooks registry.

    Registered hooks can be used in pre or post processing of models.

    Example:
        from sqlmesh.core.hooks import hook

        @hook()
        def echo(
            context: ExecutionContext,
            start: datetime,
            end: datetime,
            latest: datetime,
            **kwargs,
        ) -> None:
            print(kwargs)

    Args:
        name: A custom name for the macro, the default is the name of the function.
    """

    registry_name = "hooks"


HookRegistry = UniqueKeyDict[str, hook]
