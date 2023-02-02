import asyncio
import typing as t


async def run_in_executor(func: t.Callable) -> t.Any:
    """Run in the default loop's executor"""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, func)
