from dataclasses import dataclass
from typing import Iterable, Optional

import aiomysql

from const import FetchResult, LeaderboardTypes, Mode, Status, MapResult, MAP_FILENAME
from globs import cache
from logger import debug
from objects.beatmap import LWBeatmap, try_bmap

BASE_QUERY = """
SELECT
    s.id,
    s.{scoring},
    s.max_combo,
    s.50_count,
    s.100_count,
    s.300_count,
    s.misses_count,
    s.katus_count,
    s.gekis_count,
    s.full_combo,
    s.mods,
    s.time,
    a.username,
    a.id,
    s.pp,
    st.country
FROM
    {table} s
INNER JOIN
    users a on s.userid = a.id
INNER JOIN
    users_stats st on s.userid = st.id
WHERE
    {where_clauses}
ORDER BY {order} DESC
LIMIT {limit}
"""

BASE_COUNT = """
SELECT COUNT(*) FROM {table} s
INNER JOIN
    users a on s.userid = a.id
WHERE {where_clauses}
"""

MAX_SCORES = 500
# Structures held in cache.
@dataclass
class LeaderboardResult:
    """A cache result holding information on the cached leaderboard."""

    mode: Mode
    mods: int
    country: str
    friends_list: list
    bmap: Optional[LWBeatmap]
    scores: Iterable[tuple[object, ...]]  # TODO: better data model
    total_scores: int
    users_included: list[int]  # List of userids with scores in `scores`
    lb_type: LeaderboardTypes

    bmap_fetch: FetchResult
    lb_fetch: FetchResult

    def contains_user(self, user_id: int) -> bool:
        """Checks if a user has their scores in this result."""

        return user_id in self.users_included

    def cache(self) -> None:
        """Adds the leaderboardresult to cache."""

        # Set statuses for later.
        self.bmap_fetch = FetchResult.CACHE
        self.lb_fetch = FetchResult.CACHE

        cache_key = [self.mode, self.bmap.md5, self.lb_type]

        if self.lb_type is LeaderboardTypes.MOD:
            cache_key.append(self.mods)
        elif self.lb_type is LeaderboardTypes.COUNTRY:
            cache_key.append(self.country)
        elif self.lb_type is LeaderboardTypes.FRIENDS:
            cache_key.append(self.friends_list)

        cache.leaderboard.cache(tuple(cache_key), self)

    @staticmethod
    def empty() -> "LeaderboardResult":
        """Creates an empty LeaderboardResult."""

        return LeaderboardResult(
            Mode.VN_STANDARD,
            0,
            "XX",
            [],
            None,
            (),
            0,
            [],
            LeaderboardTypes.TOP,
            FetchResult.NONE,
            FetchResult.NONE,
        )

    @staticmethod
    def from_cache(
        mode: Mode,
        md5: str,
        lb_type: LeaderboardTypes,
        mods: int,
        country: str,
        friends_list: list,
    ) -> Optional["LeaderboardResult"]:
        """Attempts to fetch the leaderboard from the global cache."""

        cache_key = [mode, md5, lb_type]

        if lb_type is LeaderboardTypes.MOD:
            cache_key.append(mods)
        elif lb_type is LeaderboardTypes.COUNTRY:
            cache_key.append(country)
        elif lb_type is LeaderboardTypes.FRIENDS:
            cache_key.append(friends_list)

        lb = cache.leaderboard.get(tuple(cache_key))

        if lb:
            debug(f"Leaderboard {md5} {mode!r} {lb_type!r} retrieved from cache!")
        return lb

    @classmethod
    async def from_md5(
        cls,
        cur: aiomysql.Cursor,
        md5: str,
        mode: Mode,
        lb_type: LeaderboardTypes,
        filename: str,
        mods: int,
        country: str,
        friends_list: list,
    ) -> "LeaderboardResult":
        """Fetches a leaderboard directly from the MySQL database."""

        if lb := cls.from_cache(mode, md5, lb_type, mods, country, friends_list):
            return lb

        if lb := await cls.from_db(
            cur, md5, mode, lb_type, filename, mods, country, friends_list
        ):
            return lb
        return cls.empty()

    @classmethod
    async def from_db(
        cls,
        cur: aiomysql.Cursor,
        md5: str,
        mode: Mode,
        lb_type: LeaderboardTypes,
        filename: str,
        mods: int,
        country: str,
        friends_list: list,
    ) -> Optional["LeaderboardResult"]:
        """Attempts to fetch the leaderboard from MySQL."""

        bmap_res, bmap = await try_bmap(md5, cur)  # type: ignore
        bmap: LWBeatmap
        if (
            bmap_res is FetchResult.NONE
        ):  # check if map is unsubmitted or needs updating
            regexed_name = MAP_FILENAME.match(
                filename
            )  # XXX: maybe should cache this in db, have a check or something?
            if regexed_name:
                formatted_name = f"{regexed_name['artist']} - {regexed_name['title']} [{regexed_name['diff']}]"
            else:
                return MapResult.UNSUBMITTED

            try:
                await cur.execute(
                    "SELECT 1 FROM beatmaps WHERE song_name = %s", [formatted_name]
                )
                db_result = await cur.fetchone()

                if db_result:
                    return MapResult.UPDATE_REQUIRED
                else:
                    return MapResult.UNSUBMITTED
            except Exception:  # collation, we will assume update required
                return MapResult.UPDATE_REQUIRED

        where_clauses = [
            "a.privileges & 1",
            "s.beatmap_md5 = %s",
            "s.play_mode = %s",
            "s.completed = 3",
        ]
        where_args = [
            bmap.md5,
            mode.as_mode_int(),
        ]

        if lb_type == LeaderboardTypes.MOD:
            where_clauses.append("s.mods = %s")
            where_args.append(mods)
        elif lb_type == LeaderboardTypes.COUNTRY:
            where_clauses.append("st.country = %s")
            where_args.append(country)
        elif lb_type == LeaderboardTypes.FRIENDS:
            where_clauses.append("a.id IN %s")
            where_args.append(friends_list)

        where_clauses_str = " AND ".join(where_clauses)

        table = "scores_relax" if mode.relax else "scores"
        sort = "pp" if mode.relax else "score"  # should handle order AND scoring

        query = BASE_QUERY.format(
            scoring=sort,
            order=sort,
            table=table,
            where_clauses=where_clauses_str,
            limit=MAX_SCORES,
        )

        await cur.execute(query, where_args)
        scores_db = await cur.fetchall()

        # Now we work out score count.
        if (s_len := len(scores_db)) == MAX_SCORES:
            # Use MySQL
            await cur.execute(
                BASE_COUNT.format(where_clauses=where_clauses_str, table=table),
                where_args,
            )
            s_len = (await cur.fetchone())[0]

        # Create final object.
        return LeaderboardResult(
            mode=mode,
            mods=mods,
            country=country,
            friends_list=friends_list,
            bmap=bmap,
            scores=list(scores_db),
            total_scores=s_len,
            users_included=[s[13] for s in scores_db],
            lb_type=lb_type,
            bmap_fetch=bmap_res,
            lb_fetch=FetchResult.MYSQL,
        )

    def fetch_score(self, user_id: int) -> Optional[tuple[int, Optional[tuple]]]:
        """Tries to find a score from the list of a given user.
        Returns `None` if not found. Formatted in the MySQL query
        response order.

        Neat trick to sometimes avoid using another SQL query.
        """

        if user_id not in self.users_included:
            return [-1, None]
        debug("Using score leaderboards to find personal best...")

        # Iter over all to find it.
        for idx, s in enumerate(self.scores):
            if s[13] == user_id:
                return idx + 1, s
        else:
            return [-1, None]


@dataclass
class PersonalBestResult:
    """A result for a personal best cache."""

    placement: int
    score: tuple
    user_id: int
    bmap_md5: str
    mode: Mode

    def cache(self) -> None:
        """Caches the score to the global personal best score cache."""

        # TODO: index this so we can find specifics.
        cache.personal_best.cache((self.bmap_md5, self.user_id, self.mode), self)
