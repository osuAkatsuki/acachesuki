from globs.conn import conns
from globs import cache
from logger import formatted_date

import time


async def restrict_user(user_id: int, reason: str = "") -> None:
    """Restricts a user and notifies pep.py"""

    async with conns.sql.acquire() as conn:
        async with conn.cursor() as cur:
            privilege = cache.priv.get(user_id)
            if not privilege:
                privilege = await cache.priv.cache_individual(user_id, cur)

            await cur.execute(
                "SELECT username, notes FROM users WHERE id = %s", (user_id,)
            )
            data = await cur.fetchone()
            old_notes = data[1]
            notes = (old_notes or "") + f"\n[{formatted_date()}] {reason}"

            new_priv = privilege & ~1  # remove user_public
            await cur.execute(
                "UPDATE users SET privileges = %s, ban_datetime = %s, notes = %s "
                "WHERE id = %s",
                (new_priv, int(time.time()), notes, user_id),
            )
            await cur.execute(
                "INSERT INTO rap_logs (id, userid, text, datetime, through) "
                "VALUES (NULL, %s, %s, UNIX_TIMESTAMP(), %s)",
                (
                    999,
                    f'has restricted {data[0]} for the following reason: "{reason}"',
                    "Aika",
                ),
            )
            await cache.priv.cache_individual(user_id, cur)  # update in cache

            await cur.execute(
                "SELECT country FROM users_stats WHERE id = %s", (user_id,)
            )
            country = (await cur.fetchone())[0].lower()

    uid = str(user_id)
    for mode in ("std", "taiko", "ctb", "mania"):
        await conns.redis.zrem(f"ripple:leaderboard:{mode}", uid)
        await conns.redis.zrem(f"ripple:leaderboard_relax:{mode}", uid)

        if country != "xx":
            await conns.redis.zrem(f"ripple:leaderboard:{mode}:{country}", uid)
            await conns.redis.zrem(f"ripple:leaderboard_relax:{mode}:{country}", uid)

    await conns.redis.publish(
        "peppy:ban", user_id
    )  # our subscriber will pick this up alongside bancho
