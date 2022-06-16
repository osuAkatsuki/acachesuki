from dataclasses import dataclass, field
from typing import Optional, TypedDict

import aiomysql

from const import FetchResult, MAP_FILENAME, MapResult, Mode
from objects.beatmap import LWBeatmap, try_bmap
from objects.score import Score

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
    st.country,
    s.score,
    s.accuracy
FROM
    {table} s
INNER JOIN
    users a on s.userid = a.id
INNER JOIN
    users_stats st on s.userid = st.id
WHERE
    {where_clauses}
"""

BASE_COUNT = """
SELECT COUNT(*) FROM {table} s
INNER JOIN
    users a on s.userid = a.id
WHERE {where_clauses}
"""

MAX_SCORES = 500
# Structures held in cache.


class UserScore(TypedDict):
    score: Score
    rank: int


@dataclass
class Leaderboard:
    """A leaderboard"""

    mode: Mode
    lb_fetch: FetchResult
    scores: list[Score] = field(default_factory=list)

    def __len__(self) -> int:
        return len(self.scores)

    def remove_score_index(self, index: int) -> None:
        self.scores.pop(index)

    def find_user_score(self, user_id: int) -> Optional[UserScore]:
        for idx, score in enumerate(self.scores):
            if score.user_id == user_id:
                return {
                    "score": score,
                    "rank": idx + 1,
                }

    def find_score_rank(self, score_id: int) -> int:
        for idx, score in enumerate(self.scores):
            if score.id == score_id:
                return idx + 1

        return 0

    def remove_user(self, user_id: int) -> None:
        result = self.find_user_score(user_id)

        if result is not None:
            self.remove_score_index(result["rank"] - 1)

    def sort(self) -> None:
        if self.mode > Mode.VN_MANIA:
            sort = lambda score: score.pp
        else:
            sort = lambda score: score.score

        self.scores = sorted(self.scores, key=sort, reverse=True)

    def add_score(self, score: Score) -> None:
        assert score.user_id is not None
        self.remove_user(score.user_id)

        self.scores.append(score)
        self.sort()

    @classmethod
    async def from_db(
        cls,
        cur: aiomysql.Cursor,
        md5: str,
        mode: Mode,
        filename: str,
    ) -> Optional["Leaderboard"]:
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
                return None
                return MapResult.UNSUBMITTED

            try:
                await cur.execute(
                    "SELECT 1 FROM beatmaps WHERE song_name = %s", [formatted_name]
                )
                db_result = await cur.fetchone()

                if db_result:
                    return None
                    return MapResult.UPDATE_REQUIRED
                else:
                    return None
                    return MapResult.UNSUBMITTED
            except Exception:  # collation, we will assume update required
                return None
                return MapResult.UPDATE_REQUIRED

        where_clauses = [
            "s.beatmap_md5 = %s",
            "s.play_mode = %s",
            "s.completed = 3",
        ]
        where_args = [
            bmap.md5,
            mode.as_mode_int(),
        ]

        where_clauses_str = " AND ".join(where_clauses)

        table = mode.scores_table
        scoring = "pp" if mode.relax or mode.autopilot else "score"

        query = BASE_QUERY.format(
            scoring=scoring, table=table, where_clauses=where_clauses_str
        )

        await cur.execute(query, where_args)
        scores_db = await cur.fetchall()

        # Create final object.
        lb = Leaderboard(
            mode=mode,
            scores=[Score.from_lb_row(score, bmap, mode) for score in scores_db],
            lb_fetch=FetchResult.MYSQL,
        )

        lb.sort()
        return lb

    @classmethod
    async def get_leaderboard(
        cls, beatmap: LWBeatmap, cur: aiomysql.Cursor, filename: str, mode: Mode
    ) -> Optional["Leaderboard"]:
        if lb := beatmap.leaderboard.get(mode):
            lb.lb_fetch = FetchResult.CACHE
            return lb

        lb = await Leaderboard.from_db(cur, beatmap.md5, mode, filename)
        if lb is not None:
            beatmap.leaderboard[mode] = lb

        return lb
