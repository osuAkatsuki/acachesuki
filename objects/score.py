import base64
import time
from typing import Optional
from typing import TYPE_CHECKING
from urllib.parse import urlencode

import aiomysql
from aiohttp import ClientSession
from py3rijndael import RijndaelCbc
from py3rijndael import ZeroPadding

from .beatmap import LWBeatmap
from .beatmap import try_bmap
from .pp import PPUtils
from config import conf
from const import Mode
from globs import cache
from logger import info

if TYPE_CHECKING:
    from starlette.datastructures import FormData

PB_PLACEMENT = """
SELECT COUNT(*) + 1
FROM {table} s
INNER JOIN
    users a on s.userid = a.id
WHERE
    s.completed = 3 AND
    {scoring} > %s AND
    s.beatmap_md5 = %s AND
    s.play_mode = %s
"""


class Score:
    def __init__(self) -> None:
        self.id = None
        self.user_id = None
        self.user_name_real = None
        self.user_name = None
        self.score = None
        self.combo = None
        self.mods = None
        self.n300 = None
        self.n100 = None
        self.n50 = None
        self.katu = None
        self.geki = None
        self.miss = None
        self.time = None
        self.mode = None
        self.status = None
        self.acc = None
        self.pp = None
        self.checksum = None
        self.map = None
        self.previous_score = None
        self.fc = None
        self.passed = None
        self.quit = None
        self.grade = None
        self.using_patcher = None
        self.rank = None

    @staticmethod
    async def from_score_submission(
        args: "FormData", cur: aiomysql.Cursor
    ) -> Optional["Score"]:
        """Creates a score object from an osu! submission request"""

        aes = RijndaelCbc(
            key=("osu!-scoreburgr---------" + args["osuver"]).encode(),
            iv=base64.b64decode(args["iv"]),
            padding=ZeroPadding(32),
            block_size=32,
        )

        data = (
            aes.decrypt(base64.b64decode(args.getlist("score")[0])).decode().split(":")
        )

        score = Score()

        score.user_name_real = data[1]
        score.user_name = data[1].rstrip().lower().replace(" ", "_")
        score.user_id = await cache.password.check_user(
            score.user_name, args["pass"], cur
        )
        if not score.user_id:
            info(
                f"Received incorrect username + password combo from {score.user_name}."
            )
            # TODO: should this really return the score? or should it be None?
            return score

        score.map = (await try_bmap(data[0], cur))[1]
        if not score.map:
            return score

        if len(data) != 18:
            info(f"Received invalid score submission from {score.user_name}.")
            return None

        score.checksum = data[2]

        if not all(map(str.isdecimal, data[3:11] + [data[13], data[15], data[16]])):
            info(f"Received an invalid score submission from {score.user_name}")
            return None

        (
            score.n300,
            score.n100,
            score.n50,
            score.geki,
            score.katu,
            score.miss,
            score.score,
            score.combo,
        ) = map(int, data[3:11])

        score.fc = data[11] == "True"
        score.passed = data[14] == "True"
        score.quit = args.get("x") == "1"
        score.grade = data[12] if score.passed else "F"

        score.mods = int(data[13])
        mode = int(data[15])
        score.mode = Mode.from_mode_int(mode, score.mods)

        score.time = int(time.time())

        score.calc_accuracy()
        await score.calc_pp()

        await score.calc_status(cur)
        return score

    @staticmethod
    async def from_sql(
        sql_row: tuple, rx: bool, cur: aiomysql.Cursor, prev_s: bool = False
    ) -> Optional["Score"]:
        """Creates a score object from a database row"""

        score = Score()

        score.id = sql_row[0]
        score.map = (await try_bmap(sql_row[1], cur))[1]
        if not score.map and not prev_s:
            return None

        score.user_id = sql_row[2]
        score.score = sql_row[3]
        score.combo = sql_row[4]
        score.fc = sql_row[5]
        score.mods = sql_row[6]
        score.n300 = sql_row[7]
        score.n100 = sql_row[8]
        score.n50 = sql_row[9]
        score.katu = sql_row[10]
        score.geki = sql_row[11]
        score.miss = sql_row[12]
        score.time = sql_row[13]
        score.mode = Mode.from_mode_int(sql_row[14], score.mods)
        score.status = sql_row[15]
        score.acc = sql_row[16]
        score.pp = sql_row[17]
        score.passed = score.status > 0

        return score

    @staticmethod
    def from_lb_row(sql_row: tuple, beatmap: LWBeatmap, mode: Mode) -> "Score":
        score = Score()

        score.id = sql_row[0]
        score.combo = sql_row[2]
        score.n50 = sql_row[3]
        score.n100 = sql_row[4]
        score.n300 = sql_row[5]
        score.miss = sql_row[6]
        score.katu = sql_row[7]
        score.geki = sql_row[8]
        score.fc = sql_row[9]
        score.mods = sql_row[10]
        score.time = sql_row[11]
        score.user_id = sql_row[13]
        score.pp = sql_row[14]
        score.score = sql_row[16]
        score.acc = sql_row[17]

        score.mode = mode
        score.map = beatmap

        return score

    def calc_accuracy(self) -> None:
        mode = self.mode.as_mode_int()
        if mode == 0:
            hits = self.n300 + self.n100 + self.n50 + self.miss

            if hits == 0:
                self.acc = 0.0
                return
            else:
                self.acc = (
                    100.0
                    * ((self.n50 * 50.0) + (self.n100 * 100.0) + (self.n300 * 300.0))
                    / (hits * 300.0)
                )
        elif mode == 1:
            hits = self.n300 + self.n100 + self.miss

            if hits == 0:
                self.acc = 0.0
                return
            else:
                self.acc = 100.0 * ((self.n100 * 0.5) + self.n300) / hits
        elif mode == 2:
            hits = self.n300 + self.n100 + self.n50 + self.katu + self.miss

            if hits == 0:
                self.acc = 0.0
                return
            else:
                self.acc = 100.0 * (self.n300 + self.n100 + self.n50) / hits
        elif mode == 3:
            hits = self.n300 + self.n100 + self.n50 + self.geki + self.katu + self.miss

            if hits == 0:
                self.acc = 0.0
                return
            else:
                self.acc = (
                    100.0
                    * (
                        (self.n50 * 50.0)
                        + (self.n100 * 100.0)
                        + (self.katu * 200.0)
                        + ((self.n300 + self.geki) * 300.0)
                    )
                    / (hits * 300.0)
                )

    async def calc_pp(self) -> None:
        if self.mode.value == 0:  # std-vn
            calc = PPUtils.calc_rosu(self)
        elif self.mode.as_mode_int() != 0:  # pass ctb, mania and taiko thru peace
            calc = PPUtils.calc_peace(self)
        else:  # std rx & ap:
            calc = PPUtils.calc_oppai(self)

        self.pp, self.sr = await calc.calculate()

    async def calc_status(self, cur: aiomysql.Cursor) -> None:

        if self.quit:
            self.status = 0
            return
        elif not self.passed:
            self.status = 1
            return

        table = self.mode.scores_table
        query = (
            f"userid = %s AND completed = 3 AND beatmap_md5 = %s "
            f"AND play_mode = {self.mode.as_mode_int()}"
        )
        args = (
            self.user_id,
            self.map.md5,
        )

        await cur.execute(
            f"UPDATE {table} SET completed = 2 WHERE "
            + query
            + f" AND pp < {self.pp} LIMIT 1",
            args,
        )

        await cur.execute(f"SELECT 1 FROM {table} WHERE " + query + " LIMIT 1", args)
        prev = await cur.fetchone()

        if not prev:
            self.status = 3
            return

        self.status = 2

    async def first_place(self, cur: aiomysql.Cursor) -> None:
        await cur.execute(
            "DELETE FROM scores_first WHERE beatmap_md5 = %s AND mode = %s AND rx = %s",
            (
                self.map.md5,
                self.mode.as_mode_int(),
                1 if self.mode.relax else (2 if self.mode.autopilot else 0),
            ),
        )

        await cur.execute(
            "INSERT INTO scores_first (beatmap_md5, mode, rx, scoreid, userid) "
            "VALUES (%s, %s, %s, %s, %s)",
            (
                self.map.md5,
                self.mode.as_mode_int(),
                1 if self.mode.relax else (2 if self.mode.autopilot else 0),
                self.id,
                self.user_id,
            ),
        )

        # announce #1 ingame
        # TODO: check if score is highest pp play for that mode, and change announce msg if so
        profile_embed = f"[https://akatsuki.pw/u/{self.user_id} {self.user_name_real}]"
        ann_msg = f"[{'R' if self.mode.relax else ('V' if not self.mode.autopilot else 'A')}] {profile_embed} achieved rank #1 on {self.map.embed} ({self.mode.annouce_prefix}) - {self.pp:.2f}pp"

        # send request to pep.py
        params = urlencode({"k": conf.fokabot_key, "to": "#announce", "msg": ann_msg})

        async with ClientSession() as sesh:
            await sesh.get(
                f"http://localhost:5001/api/v1/fokabotMessage?{params}", timeout=2
            )

    async def submit(self, cur: aiomysql.Cursor) -> None:
        table = self.mode.scores_table

        await cur.execute(
            f"INSERT INTO {table} (beatmap_md5, userid, score, max_combo, full_combo, "
            "mods, 300_count, 100_count, 50_count, katus_count, gekis_count, misses_count, "
            "time, play_mode, completed, accuracy, pp, checksum, patcher) VALUES "
            "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                self.map.md5,
                self.user_id,
                self.score,
                self.combo,
                int(self.fc),
                self.mods,
                self.n300,
                self.n100,
                self.n50,
                self.katu,
                self.geki,
                self.miss,
                self.time,
                self.mode.as_mode_int(),
                self.status,
                self.acc,
                self.pp,
                self.checksum,
                1 if self.using_patcher else 0,
            ),
        )

        self.id = cur.lastrowid
