# Structures related to caching etc.
import asyncio
import time
from typing import Optional
from typing import TypedDict
from typing import Union

import aiomysql
import bcrypt

from const import Mode
from objects.stats import Stats

CACHE_KEY = Union[int, str, tuple]


class CachedObject(TypedDict):
    expire: int
    object: object


class LRUCache:  # generic class
    """A key-value store implementing LRU eviction."""

    def __init__(self, cache_length: int = 5, cache_limit: int = 500) -> None:
        """Establishes a cache and configures the limits.
        Args:
            cache_length (int): How long (in minutes) each cache lasts before
                being removed
            cache_limit (int): A limit to how many objects can be max cached
                before other objects start being removed.
        """
        self._cache: dict[CACHE_KEY, CachedObject] = {}  # The main cache object.
        self.length = (
            cache_length * 60
        )  # Multipled by 60 to get the length in seconds rather than minutes.
        self._cache_limit = cache_limit

    @property
    def cached_items(self) -> int:
        """Returns an int of the lumber of cached items stored."""

        return len(self._cache)

    def __len__(self) -> int:
        return self.cached_items

    def cache(self, key: CACHE_KEY, cache_obj: object) -> None:
        """Adds an object to the cache."""
        self._cache[key] = {
            "expire": int(time.time()) + self.length,
            "object": cache_obj,
        }
        self.run_checks()

    def remove_cache(self, key: CACHE_KEY) -> None:
        """Removes an object from cache."""
        try:
            del self._cache[key]
        except KeyError:
            # It doesnt matter if it fails. All that matters is that no such object exist and if it doesnt exist in the first place, that's already objective complete.
            pass

    def get_lb_caches(self, identifier: CACHE_KEY) -> list:
        caches = []

        for key in self._get_cached_keys():
            if len(key) < 3:
                continue
            if key[:2] == identifier:
                caches.append(key)

        return caches

    def remove_lb_cache(self, identifier: CACHE_KEY) -> None:
        """Assisting func to remove all types of lbs on a map"""

        caches = self.get_lb_caches(identifier)
        for cache in caches:
            self.remove_cache(cache)

    def get(self, key: CACHE_KEY) -> Optional[object]:
        """Retrieves a cached object from cache."""

        # Try to get it from cache.
        curr_obj = self._cache.get(key)

        if curr_obj is not None:
            return curr_obj["object"]

    def remove_all_elements(self, pattern: str) -> None:
        # remove all tuple entries with this as a starter

        for key in self._get_cached_keys():
            if isinstance(key, tuple) and key[0] == pattern:
                self.remove_cache(key)

    def _get_cached_keys(self) -> tuple[CACHE_KEY, ...]:
        """Returns a list of all cache keys currently cached."""
        return tuple(self._cache)

    def _get_expired_cache(self) -> list:
        """Returns a list of expired cache keys."""
        current_timestamp = int(time.time())
        expired = []
        for key in self._get_cached_keys():
            # We dont want to use get as that  will soon have the ability to make its own objects, slowing this down.
            if self._cache[key]["expire"] < current_timestamp:
                # This cache is expired.
                expired.append(key)
        return expired

    def _remove_expired_cache(self) -> None:
        """Removes all of the expired cache."""
        for key in self._get_expired_cache():
            self.remove_cache(key)

    def _remove_limit_cache(self) -> None:
        """Removes all objects past limit if cache reached its limit."""

        # Calculate how much objects we have to throw away.
        throw_away_count = len(self._get_cached_keys()) - self._cache_limit

        if not throw_away_count:
            # No levels to throw away
            return

        # Get x oldest ids to remove.
        throw_away_ids = self._get_cached_keys()[:throw_away_count]
        for key in throw_away_ids:
            self.remove_cache(key)

    def run_checks(self) -> None:
        """Runs checks on the cache."""
        self._remove_expired_cache()
        self._remove_limit_cache()

    def get_all_items(self):
        """Generator that lists all of the objects currently cached."""

        # return [obj["object"] for _, obj in self._cache.items()]

        # Make it a generator for performance.
        for obj in self._cache.values():
            yield obj["object"]

    def get_all_keys(self):
        """Generator that returns all keys of the keys to the cache."""

        return self._get_cached_keys()


class AsyncLRUCache:  # generic class
    """A key-value store implementing LRU eviction. Implements an async lock."""

    def __init__(self, cache_length: int = 5, cache_limit: int = 500) -> None:
        """Establishes a cache and configures the limits.
        Args:
            cache_length (int): How long (in minutes) each cache lasts before
                being removed
            cache_limit (int): A limit to how many objects can be max cached
                before other objects start being removed.
        """
        self._cache: dict[CACHE_KEY, CachedObject] = {}  # The main cache object.
        self.length = (
            cache_length * 60
        )  # Multipled by 60 to get the length in seconds rather than minutes.
        self._cache_limit = cache_limit
        self._lock = asyncio.Lock()

    @property
    def cached_items(self) -> int:
        """Returns an int of the lumber of cached items stored."""

        return len(self._cache)

    def __len__(self) -> int:
        return self.cached_items

    def cache(self, key: CACHE_KEY, cache_obj: object) -> None:
        """Adds an object to the cache."""
        self._cache[key] = {
            "expire": int(time.time()) + self.length,
            "object": cache_obj,
        }
        self.run_checks()

    async def remove_cache(self, key: CACHE_KEY, lock: bool = True) -> None:
        """Removes an object from cache."""
        if lock:
            await self._lock.acquire()
        try:
            del self._cache[key]
        except KeyError:
            # It doesnt matter if it fails. All that matters is that no such object exist and if it doesnt exist in the first place, that's already objective complete.
            pass
        finally:
            self._lock.release()

    def get_lb_caches(self, identifier: CACHE_KEY) -> list:
        caches = []

        for key in self._get_cached_keys():
            if len(key) < 3:
                continue
            if key[:2] == identifier:
                caches.append(key)

        return caches

    async def remove_lb_cache(self, identifier: CACHE_KEY) -> None:
        """Assisting func to remove all types of lbs on a map"""

        async with self._lock:
            caches = self.get_lb_caches(identifier)
            for cache in caches:
                self.remove_cache(cache, False)

    async def get(self, key: CACHE_KEY) -> Optional[object]:
        """Retrieves a cached object from cache."""

        # Try to get it from cache.
        async with self._lock:
            curr_obj = self._cache.get(key)

            if curr_obj is not None:
                return curr_obj["object"]

    async def remove_all_elements(self, pattern: str) -> None:
        # remove all tuple entries with this as a starter

        async with self._lock:
            for key in self._get_cached_keys():
                if isinstance(key, tuple) and key[0] == pattern:
                    self.remove_cache(key, False)

    def _get_cached_keys(self) -> tuple[CACHE_KEY, ...]:
        """Returns a list of all cache keys currently cached."""
        return tuple(self._cache)

    def _get_expired_cache(self) -> list:
        """Returns a list of expired cache keys."""
        current_timestamp = int(time.time())
        expired = []
        for key in self._get_cached_keys():
            # We dont want to use get as that  will soon have the ability to make its own objects, slowing this down.
            if self._cache[key]["expire"] < current_timestamp:
                # This cache is expired.
                expired.append(key)
        return expired

    async def _remove_expired_cache(self) -> None:
        """Removes all of the expired cache."""

        async with self._lock:
            for key in self._get_expired_cache():
                self.remove_cache(key, False)

    async def _remove_limit_cache(self) -> None:
        """Removes all objects past limit if cache reached its limit."""

        async with self._lock:
            # Calculate how much objects we have to throw away.
            throw_away_count = len(self._get_cached_keys()) - self._cache_limit

            if not throw_away_count:
                # No levels to throw away
                return

            # Get x oldest ids to remove.
            throw_away_ids = self._get_cached_keys()[:throw_away_count]
            for key in throw_away_ids:
                self.remove_cache(key)

    def run_checks(self) -> None:
        """Runs checks on the cache."""
        self._remove_expired_cache()
        self._remove_limit_cache()

    async def get_all_items(self):
        """Generator that lists all of the objects currently cached."""

        # return [obj["object"] for _, obj in self._cache.items()]

        # Make it a generator for performance.
        async with self._lock:
            for obj in self._cache.values():
                yield obj["object"]

    def get_all_keys(self):
        """Generator that returns all keys of the keys to the cache."""

        return self._get_cached_keys()


class ClanCache:
    """A cache for storing the clan tags of users for quick lookups."""

    def __init__(self) -> None:
        self._cache: dict[int, str] = {}

    async def preload_all(self, cur: aiomysql.Cursor) -> None:
        """Loads all clan tags for users."""

        self._cache.clear()

        # Grab all clan memberships from db.
        await cur.execute(
            "SELECT u.id, c.tag FROM users u " "INNER JOIN clans c ON u.clan_id = c.id"
        )
        clans_db = await cur.fetchall()

        # Save all to cache.
        for u, tag in clans_db:
            self._cache[u] = tag

    def get(self, user_id: int) -> Optional[str]:
        """Returns the clan tag for the given user.

        Args:
            user_id (int): The user you want to grab the clan tag for.
        """

        return self._cache.get(user_id)

    async def cache_individual(self, user_id: int, cur: aiomysql.Cursor) -> None:
        """Caches an individual's clan (singular person) to cache. Meant for
        handling clan updates.

        Args:
            user_id (int): The user for who to update the cached tag for.
        """

        # Delete them if they already had a value cached.
        try:
            del self._cache[user_id]
        except KeyError:
            pass

        # Grab their tag.
        await cur.execute(
            "SELECT c.tag FROM clans c INNER JOIN "
            "user_clans uc ON c.id = uc.clan WHERE uc.user = %s LIMIT 1",
            (user_id,),
        )

        clan_db = await cur.fetchone()

        if not clan_db:
            return  # Nothing... Keep it empty and get will just return None.

        # cache their tag.
        self._cache[user_id] = clan_db[0]

    @property
    def cached_count(self) -> int:
        """Number of tags cached."""

        return len(self._cache)


class BCryptCache:
    """A cache for storing known password md5s to speed up the auth process."""

    def __init__(self) -> None:
        # safe_username: (user_id, known_md5)
        self._cache: dict[str, tuple[int, str]] = {}

    async def check_user(
        self, safe_name: str, pw_md5: str, cur: aiomysql.Cursor
    ) -> int:
        """Checks a username, password combination, managing caching.

        Args:
            safe_name (str): The user's username in Ripple's 'safe' format.
            pw_md5 (str): The user's password hashed using the MD5 hash.


        Returns:
            `int` of the user's id on auth success, else `0`.
        """

        # Check if we can alr check here real quick.
        if cached_res := self._cache.get(safe_name):
            # We cant just return `cached_res == pw_md5` due to us not having a
            # pubsub for password changes
            if cached_res[1] == pw_md5:
                return cached_res[0]

        # MySQL time!
        await cur.execute(
            "SELECT id, password_md5 FROM users WHERE username_safe = %s LIMIT 1",
            (safe_name,),
        )

        res_db = await cur.fetchone()

        if not res_db:
            return 0

        # Bcrypt check (MAY TAKE UP TO 300ms! This is why we cache it in the first place)
        if bcrypt.checkpw(pw_md5.encode(), res_db[1].encode()):
            # Great success! Cache it now.
            self._cache[safe_name] = (res_db[0], pw_md5)
            return res_db[0]

        return 0


class PrivilegeCache:
    """A cache for storing the privileges of users for quick lookups."""

    def __init__(self) -> None:
        self._cache: dict[int, int] = {}

    async def preload_all(self, cur: aiomysql.Cursor) -> None:
        """Loads all privileges for users."""

        self._cache.clear()

        # Grab all privileges from db.
        await cur.execute("SELECT id, privileges FROM users")
        privs_db = await cur.fetchall()

        # Save all to cache.
        for u, priv in privs_db:
            self._cache[u] = priv

    def get(self, user_id: int) -> Optional[int]:
        """Returns the privileges for the given user.

        Args:
            user_id (int): The user you want to grab the privileges for.
        """

        return self._cache.get(user_id)

    async def cache_individual(self, user_id: int, cur: aiomysql.Cursor) -> int:
        """Caches an individual's privilege to cache. Meant for
        handling privilege updates.

        Args:
            user_id (int): The user for who to update the cached privilege for.
        """

        # Delete them if they already had a value cached.
        try:
            del self._cache[user_id]
        except KeyError:
            pass

        # Grab their priv.
        await cur.execute(
            "SELECT privileges FROM users WHERE id = %s",
            (user_id,),
        )

        priv_db = await cur.fetchone()

        if not priv_db:
            raise type("WTFError", (Exception,), {"message": "No user found!"})

        # cache their priv.
        self._cache[user_id] = priv_db[0]

        return priv_db[0]

    @property
    def cached_count(self) -> int:
        """Number of tags cached."""

        return len(self._cache)


class CountryCache:
    """A cache for storing the country of users for quick lookups."""

    def __init__(self) -> None:
        self._cache: dict[int, str] = {}

    async def preload_all(self, cur: aiomysql.Cursor) -> None:
        """Loads all countries for users."""

        self._cache.clear()

        # Grab all countries from db.
        await cur.execute("SELECT id, country FROM users_stats")
        countries_db = await cur.fetchall()

        # Save all to cache.
        for u, country in countries_db:
            self._cache[u] = country

    def get(self, user_id: int) -> Optional[str]:
        """Returns the country for the given user.

        Args:
            user_id (int): The user you want to grab the country for.
        """

        return self._cache.get(user_id)

    async def cache_individual(self, user_id: int, cur: aiomysql.Cursor) -> str:
        """Caches an individual's country to cache. Meant for
        handling privilege updates.

        Args:
            user_id (int): The user for who to update the cached country for.
        """

        # Delete them if they already had a value cached.
        try:
            del self._cache[user_id]
        except KeyError:
            pass

        # Grab their priv.
        await cur.execute(
            "SELECT country FROM users_stats WHERE id = %s",
            (user_id,),
        )

        country_db = await cur.fetchone()

        if not country_db:
            raise type("WTFError", (Exception,), {"message": "No user found!"})

        # cache their country.
        self._cache[user_id] = country_db[0]

    @property
    def cached_count(self) -> int:
        """Number of tags cached."""

        return len(self._cache)


class FriendsCache:
    """A cache for storing the friends list of users for quick lookups."""

    def __init__(self) -> None:
        self._cache: dict[int, list[int]] = {}

    def get(self, user_id: int) -> Optional[list[int]]:
        """Returns the friends list for the given user.

        Args:
            user_id (int): The user you want to grab the friends list for.
        """

        return self._cache.get(user_id)

    async def cache_individual(self, user_id: int, cur: aiomysql.Cursor) -> None:
        """Caches an individual's friend list to cache. Meant for
        handling friends list updates.

        Args:
            user_id (int): The user for who to update the cached friends list for.
        """

        # Delete them if they already had a value cached.
        try:
            del self._cache[user_id]
        except KeyError:
            pass

        # Grab their friends list.
        await cur.execute(
            "SELECT user2 FROM users_relationships WHERE user1 = %s",
            (user_id,),
        )

        f_db = await cur.fetchall()

        # cache their country.
        self._cache[user_id] = f_db or [user_id]

    @property
    def cached_count(self) -> int:
        """Number of tags cached."""

        return len(self._cache)


class WhitelistCache:
    """A cache for storing the whitelist status of users for quick lookups."""

    def __init__(self) -> None:
        self._cache: dict[int, int] = {}

    async def preload_all(self, cur: aiomysql.Cursor) -> None:
        """Loads all whitelist status' for users."""

        self._cache.clear()

        # Grab all whitelist status' from db.
        await cur.execute("SELECT id, whitelist FROM users")
        w_db = await cur.fetchall()

        # Save all to cache.
        for u, status in w_db:
            self._cache[u] = status

    def get(self, user_id: int, relax: bool) -> bool:
        """Returns the whitelist status for the given user.

        Args:
            user_id (int): The user you want to grab the whitelist status for.
        """

        val = self._cache.get(user_id)
        if not val:
            return False

        if not relax:
            return val & 1 == 1
        else:
            return val & 2 == 2

    async def cache_individual(self, user_id: int, cur: aiomysql.Cursor) -> None:
        """Caches an individual's whitelist status to cache. Meant for
        handling whitelist status updates.

        Args:
            user_id (int): The user for who to update the cached whitelist status for.
        """

        # Delete them if they already had a value cached.
        try:
            del self._cache[user_id]
        except KeyError:
            pass

        # Grab their whitelist status.
        await cur.execute(
            "SELECT whitelist FROM users WHERE id = %s",
            (user_id,),
        )

        w_db = await cur.fetchone()

        # cache their status.
        self._cache[user_id] = w_db

    @property
    def cached_count(self) -> int:
        """Number of tags cached."""

        return len(self._cache)


class StatsCache:
    """A cache for storing the stats of users for quick lookups."""

    def __init__(self) -> None:
        self._cache: dict[tuple[int, Mode], "Stats"] = {}

    async def get(self, user_id: int, mode: Mode, cur: aiomysql.Cursor) -> "Stats":
        """Returns the stats for the given user.

        Args:
            user_id (int): The user you want to grab the stats for.
        """

        val = self._cache.get(
            (
                user_id,
                mode,
            )
        )

        if val:
            return val

        await self.cache_individual(user_id, mode, cur)
        val = self._cache.get(
            (
                user_id,
                mode,
            )
        )
        assert val is not None
        return val

    async def cache_individual(
        self, user_id: int, mode: Mode, cur: aiomysql.Cursor
    ) -> None:
        """Caches an individual's stats to cache. Meant for
        handling stats updates.

        Args:
            user_id (int): The user for who to update the cached stats for.
        """

        # Delete them if they already had a value cached.
        try:
            del self._cache[(user_id, mode)]
        except KeyError:
            pass

        # Grab their stats.
        stats = await Stats.from_db(cur, user_id, mode)

        # cache their status.
        self._cache[
            (
                user_id,
                mode,
            )
        ] = stats

    @property
    def cached_count(self) -> int:
        """Number of tags cached."""

        return len(self._cache)
