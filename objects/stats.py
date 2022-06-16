from dataclasses import dataclass

from globs.conn import conns
from globs import cache
from const import Mode

import aiomysql


@dataclass
class Stats:
    user_id: int
    mode: Mode

    ranked_score: int
    total_score: int
    pp: float
    accuracy: float
    playcount: int
    total_hits: int

    rank: int = 0
    _recalc_pp: int = 0

    @staticmethod
    async def from_db(cur: aiomysql.Cursor, user_id: int, mode: Mode) -> "Stats":
        """Creates a stats object from a database row"""

        table = "rx_stats" if mode.relax else "users_stats"
        await cur.execute(
            "SELECT ranked_score_{m} ranked_score, total_score_{m} total_score, pp_{m} pp, avg_accuracy_{m} accuracy,"
            "playcount_{m} playcount, total_hits_{m} total_hits FROM {table} WHERE id = %s".format(
                m=mode.db_prefix, table=table
            ),
            (user_id,),
        )

        db_stat = await cur.fetchone()
        if not db_stat:
            return

        stats = Stats(user_id, mode, *db_stat)

        await stats.get_rank()
        return stats

    async def get_rank(self) -> None:
        board = "relaxboard" if self.mode.relax else "leaderboard"
        mode = self.mode.db_prefix

        rank = await conns.redis.zrevrank(f"ripple:{board}:{mode}", self.user_id)
        self.rank = int(rank) + 1 if rank else 0

    async def update_rank(self) -> None:
        board = "relaxboard" if self.mode.relax else "leaderboard"
        mode = self.mode.db_prefix

        country = cache.country.get(self.user_id)

        await conns.redis.zadd(f"ripple:{board}:{mode}", self.pp, self.user_id)
        if country and country.lower() != "xx":
            await conns.redis.zadd(
                f"ripple:{board}:{mode}:{country.lower()}", self.pp, self.user_id
            )

        await self.refresh_stats()

    async def save(self, cur: aiomysql.Cursor) -> None:
        await cur.execute(
            "UPDATE {table} SET ranked_score_{m} = %s, total_score_{m} = %s, "
            "pp_{m} = %s, avg_accuracy_{m} = %s, playcount_{m} = %s, "
            "total_hits_{m} = %s WHERE id = %s".format(
                m=self.mode.db_prefix,
                table="users_stats" if not self.mode.relax else "rx_stats",
            ),
            (
                self.ranked_score,
                self.total_score,
                self.pp,
                self.accuracy,
                self.playcount,
                self.total_hits,
                self.user_id,
            ),
        )

        await self.refresh_stats()

    async def refresh_stats(self) -> None:
        await conns.redis.publish("peppy:update_cached_stats", self.user_id)

    async def recalc(self, cur: aiomysql.Cursor, score_pp: int = None) -> None:

        table = "scores" if not self.mode.relax else "scores_relax"
        total_pp = self.pp
        play_mode = self.mode.as_mode_int()

        if not self._recalc_pp or score_pp > self._recalc_pp:
            await cur.execute(
                f"SELECT s.pp FROM {table} s RIGHT JOIN beatmaps b USING(beatmap_md5) "
                "WHERE s.completed = 3 AND s.play_mode = %s AND b.ranked in (3, 2) AND s.userid = %s "
                "AND pp IS NOT NULL "
                "ORDER BY s.pp DESC LIMIT 125",
                (play_mode, self.user_id),
            )
            scores_pp = await cur.fetchall()

            total_pp = 0
            for idx_pp, pp in enumerate(scores_pp):
                total_pp += round(round(pp[0]) * 0.95 ** idx_pp)

            if idx_pp == 124:
                self._recalc_pp = pp

        sortby = "pp"
        if play_mode != 0:
            sortby = "accuracy"
        await cur.execute(
            f"SELECT accuracy FROM {table} WHERE userid = %s AND play_mode = %s AND completed = 3 ORDER BY {sortby} DESC LIMIT 500",
            (self.user_id, play_mode),
        )
        scores_acc = await cur.fetchall()

        total_acc = 0.0
        divider = 0.0
        try:
            for idx, acc in enumerate(scores_acc):
                total_acc += acc[0] * int((0.95 ** idx) * 100)
                divider += int((0.95 ** idx) * 100)
            self.accuracy = total_acc / divider
        except Exception as e:
            print(e)
            self.accuracy = 0.0
        self.pp = total_pp
