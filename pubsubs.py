# Our redis pubsub management.
import asyncio
import traceback
from typing import Callable

from aioredis import Channel

from const import Status
from globs.cache import beatmap
from globs.conn import conns
from logger import error
from logger import info

REDIS_LOCK = asyncio.Lock()


async def wait_for_pub(ch: Channel, h: Callable) -> None:
    """A permanently looping task waiting for the call of a `publish` redis
    event, calling its respective handler upon recevial. Meant to be ran as
    a task.

    Args:
        ch (Channel): The publish channel to listen and read from.
        h (Callable): The async
    """

    async for msg in ch.iter():
        try:
            await h(msg)
        except Exception:
            error("Exception occured while handling pubsub! " + traceback.format_exc())


async def pubsub_executor(name: str, h: Callable) -> None:
    """Creates an loop task listening to a redis channel with the name `name`
    upon creating it, listening to `publish` events. Upon receival, calls `h`.
    """

    (ch,) = await conns.redis.subscribe(name)
    asyncio.get_running_loop().create_task(wait_for_pub(ch, h))


async def handle_status_update(msg) -> None:
    md5, new_status = msg.decode().split(",")

    beatmap.remove_cache(md5)

    info(
        f"Received status update on beatmap {md5} with new status {Status(int(new_status))!r}"
    )
