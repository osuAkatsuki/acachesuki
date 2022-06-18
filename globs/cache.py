# Global caches that should be accessable everywhere.
from .conn import conns
from logger import info
from objects.cache import BCryptCache
from objects.cache import ClanCache
from objects.cache import CountryCache
from objects.cache import FriendsCache
from objects.cache import LRUCache
from objects.cache import PrivilegeCache
from objects.cache import StatsCache
from objects.cache import WhitelistCache

# -- Basic caches --
beatmap = LRUCache(cache_length=120, cache_limit=1000)

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
                    "ap": cap[5],
                    "apfl": cap[6],
                }

            info(f"Loaded pp limits cache!")
