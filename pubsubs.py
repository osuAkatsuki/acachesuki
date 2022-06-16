# Our redis pubsub management.
import asyncio
import traceback
from typing import Callable

from aioredis import Channel

from json import loads
from const import Mode, Status
from globs.cache import leaderboard, personal_best, beatmap
from globs.conn import conns
from logger import error, info
from objects.beatmap import try_bmap
from objects.leaderboards import LeaderboardResult


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
    for key, lb in leaderboard._cache.copy().items():
        if lb["object"].bmap.md5 == md5:
            try:
                del leaderboard._cache[key]
            except KeyError:
                pass

    info(
        f"Received status update on beatmap {md5} with new status {Status(new_status)!r}"
    )


async def handle_restricted_user(msg) -> None:
    user_id = int(msg.decode())

    # uhhhhhhhhhhhhhhhhhhhhhh this sucks
    for key in leaderboard.get_all_keys():
        val = leaderboard.get(key)
        if not isinstance(val, LeaderboardResult):
            continue

        _, score = val.fetch_score(user_id)
        if score:
            val.scores.remove(score)
            val.total_scores -= 1

            val.users_included.remove(
                user_id
            ) if user_id in val.users_included else None  # lol

    info(f"Received restriction pubsub for user ID {user_id}")


async def name_change(msg) -> None:
    # Parse JSON formatted data.
    j_data = loads(msg.decode())
    user_id = int(j_data["userID"])
    async with conns.sql.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT username FROM users WHERE id = %s", user_id)
            new_username = (await cur.fetchone())[0]

    for key in leaderboard.get_all_keys():
        val = leaderboard.get(key)
        if not isinstance(val, LeaderboardResult):
            continue

        _, score = val.fetch_score(user_id)
        if score:
            score_copy = list(score)
            val.scores.remove(score)
            score_copy[12] = new_username
            val.scores.append(tuple(score_copy))

    info(f"Handled user ID: {user_id} name change to -> {new_username}")
