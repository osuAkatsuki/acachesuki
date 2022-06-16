# Global caches that should be accessable everywhere.
from logger import info
from objects.cache import (
    BCryptCache,
    ClanCache,
    CountryCache,
    FriendsCache,
    LRUCache,
    PrivilegeCache,
    WhitelistCache,
    StatsCache,
)

from .conn import conns

# -- Basic caches --
beatmap = LRUCache(cache_length=120, cache_limit=1000)

# Stores tuples (mode, md5, lb_type)
leaderboard = LRUCache(cache_length=120, cache_limit=100_000)

personal_best = LRUCache(cache_length=240, cache_limit=100_000)

# -- Specialised Caches --
clan = ClanCache()
password = BCryptCache()
priv = PrivilegeCache()
country = CountryCache()
friends = FriendsCache()
whitelist = WhitelistCache()
stats = StatsCache()

# Maps that obv dont exist. md5: Status
no_check_md5s = {}

# pp limits to cause auto-restrictions
pp_caps = {}


async def init_caches():
    """Pre-loads all the specialised caches with data for smooth operation."""

    async with conns.sql.acquire() as conn:
        async with conn.cursor() as cur:
            await clan.preload_all(cur)
            info(f"Loaded clan cache with {clan.cached_count} cached entries!")

            await priv.preload_all(cur)
            info(f"Loaded privilege cache with {priv.cached_count} cached entries!")

            await country.preload_all(cur)
            info(f"Loaded country cache with {country.cached_count} cached entries!")

            await whitelist.preload_all(cur)
            info(
                f"Loaded whitelist cache with {whitelist.cached_count} cached entries!"
            )

            # too lazy to make an object for this, also unnecessary
            await cur.execute("SELECT * FROM pp_limits")
            caps = await cur.fetchall()

            for mode in (
                0,
                1,
                2,
                3,
            ):
                cap = [row for row in caps if row[0] == mode][0]
                pp_caps[mode] = {
                    "vn": cap[1],
                    "vnfl": cap[3],
                    "rx": cap[2],
                    "rxfl": cap[4],
                }

            info(f"Loaded pp limits cache!")
