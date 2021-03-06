import random
import traceback
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timedelta
from typing import Optional
from typing import TYPE_CHECKING
from typing import Union

import aiohttp
import aiomysql  # For type checking

from config import conf
from const import FetchResult
from const import Mode
from const import Status
from globs import cache
from logger import debug
from logger import error

if TYPE_CHECKING:
    from objects.leaderboards import Leaderboard

try:
    import orjson as json_lib
except ImportError:
    import json as json_lib

UPDATE_CHECK_TIME = 432000  # 5 Days in seconds
UPDATE_SKIP_STATUSES = (Status.RANKED, Status.APPROVED)

json_dump = lambda data: str(json_lib.dumps(data))

SQL_BMAP_FETCH_QUERY = (
    "SELECT beatmap_id, beatmapset_id, beatmap_md5, ranked, "
    "song_name, rating, latest_update, ranked_status_freezed, playcount, passcount "
    "FROM beatmaps WHERE beatmap_md5 = %s LIMIT 1"
)


def create_song_name(artist: str, title: str, diff: str) -> str:
    """Creates a Ripple format song_name for use in the database."""

    return f"{artist} - {title} [{diff}]"


async def get_bmap(md5: str) -> Union[dict, str]:
    """Creates a request to Akatsuki's oapi bmap mirror (http://akat.fumos.live/get_map)
    and returns the json response."""

    async with aiohttp.ClientSession(json_serialize=json_dump) as s:
        key = random.choice(conf.osu_api_keys)
        async with s.get(f"https://old.ppy.sh/api/get_beatmaps?h={md5}&k={key}") as r:
            return (await r.json(content_type=None))[0]  # NOTE: may raise exc


@dataclass
class LWBeatmap:
    """A low-weight beatmap object used for leaderboard requests."""

    id: int
    set_id: int
    md5: str
    status: Status
    song_name: str
    rating: float
    last_updated: datetime
    frozen: bool

    playcount: int
    passcount: int

    leaderboard: dict[Mode, "Leaderboard"] = field(default_factory=dict)

    @property
    def deserves_update(self) -> bool:
        """Bool corresponding to whether the server should try to fetch updated
        details for the map."""

        # See if we can skip calculating scaling etc
        if self.frozen:
            return False

        # Linear time scaling from gulag.
        now = datetime.now()
        update_delta = now - self.last_updated

        check_delta = timedelta(hours=2 + ((5 / 365) * update_delta.days))

        return now > (self.last_updated + check_delta)

    @property
    def has_leaderboard(self) -> bool:
        return self.status >= Status.RANKED

    @property
    def gives_pp(self) -> bool:
        return self.has_leaderboard and self.status != Status.LOVED

    @property
    def formatted_time(self) -> str:
        return self.last_updated.strftime("%Y-%m-%d %H:%M:%S")

    @property
    def ts(self) -> float:
        return self.last_updated.timestamp()

    @property
    def url(self) -> str:
        return f"https://osu.ppy.sh/beatmapsets/{self.set_id}#osu/{self.id}"

    @property
    def embed(self) -> str:
        return f"[{self.url} {self.song_name}]"

    def cache(self) -> None:
        """Adds the current beatmap object to the global beatmap cache."""

        cache.beatmap.cache(self.md5, self)

        debug(f"Cached beatmap {self.song_name} to the global beatmap cache.")

    async def save(self, cur: aiomysql.Cursor) -> None:
        """Adds the current beatmap object into the database, updating if it already exists"""

        await cur.execute(
            "REPLACE INTO beatmaps (beatmap_id, beatmapset_id, beatmap_md5, ranked, "
            "song_name, rating, latest_update, ranked_status_freezed, playcount, passcount) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (
                self.id,
                self.set_id,
                self.md5,
                self.status,
                self.song_name,
                self.rating,
                self.ts,
                self.frozen,
                self.playcount,
                self.passcount,
            ),
        )

        debug(f"Saved beatmap {self.song_name} to the database.")

    # Staticmethods
    @staticmethod
    def from_cache(md5: str) -> Optional["LWBeatmap"]:
        """Attempts to fetch an already created instance of `LWBeatmap` from the
        global cache. If not already cached, returns `None`."""

        if bmap := cache.beatmap.get(md5):
            debug(f"Retrieved beatmap {bmap.song_name} from cache!")

        return bmap

    @staticmethod
    async def from_db(md5: str, cur: aiomysql.Cursor) -> Optional["LWBeatmap"]:
        """Attmepts to create an instance of `LWBeatmap` using data acquired
        from the MySQL database. If does not exist in the database, returns `None`."""

        await cur.execute(SQL_BMAP_FETCH_QUERY, md5)
        bmap_db = await cur.fetchone()

        if not bmap_db:
            return None

        return LWBeatmap(
            id=bmap_db[0],
            set_id=bmap_db[1],
            md5=bmap_db[2],
            status=Status(bmap_db[3]),
            song_name=bmap_db[4],
            rating=bmap_db[5],
            last_updated=datetime.fromtimestamp(bmap_db[6]),
            frozen=bmap_db[7],
            playcount=bmap_db[8],
            passcount=bmap_db[9],
        )

    @staticmethod
    def from_oapiv1_dict(resp: dict) -> "LWBeatmap":
        """Creates an instance of `LWBeatmap` using data from the dict `resp`
        formatted in the style of an osu!api v1 JSON response."""

        return LWBeatmap(
            id=int(resp["beatmap_id"]),
            set_id=int(resp["beatmapset_id"]),
            md5=resp["file_md5"],
            status=Status.from_api(int(resp["approved"])),
            song_name=create_song_name(resp["artist"], resp["title"], resp["version"]),
            rating=10,
            last_updated=datetime.strptime(resp["last_update"], "%Y-%m-%d %H:%M:%S"),
            frozen=False,
            playcount=0,
            passcount=0,
        )

    @staticmethod
    async def from_akat_mirror(md5: str) -> Optional["LWBeatmap"]:
        """Attempts to create an instance of `LWBeatmap` from Akatsuki's osu!api
        beatmap mirror. Returns `None` if not found."""

        try:
            resp = await get_bmap(md5)
        except IndexError:
            # 0 maps returned
            return None
        except Exception:
            error(
                f"Error sending request to Akat osu!api mirror with md5 {md5}\n"
                + traceback.format_exc()
            )
            return None

        if not resp or isinstance(resp, str):
            debug(f"Beatmap {md5} does not exist on the osu!api!")
            return None

        return LWBeatmap.from_oapiv1_dict(resp)

    async def increment_counts(self, cur: aiomysql.Cursor) -> None:
        """Increments the playcount and passcount of the current beatmap object."""

        self.playcount += 1
        self.passcount += 1

        await cur.execute(
            "UPDATE beatmaps SET playcount = %s, passcount = %s "
            "WHERE beatmap_md5 = %s",
            (self.playcount, self.passcount, self.md5),
        )

        debug(f"Incremented playcount and passcount of {self.song_name}.")

    @staticmethod
    def blank_with_status(st: Status) -> "LWBeatmap":
        """Creates a blank object with a status."""

        return LWBeatmap(
            id=0,
            set_id=0,
            md5="",
            status=st,
            song_name="",
            rating=0.0,
            last_updated=datetime.now(),
            frozen=False,
            playcount=0,
            passcount=0,
        )

    def __repr__(self) -> str:
        return f"{self.song_name} ({self.md5})"


UPDATE_BMAP = LWBeatmap.blank_with_status(Status.UPDATE_AVAILABLE)
NOT_SUB_BMAP = LWBeatmap.blank_with_status(Status.NOT_SUBMITTED)

# Big mess im tired.
async def try_bmap(
    md5: str, cur: aiomysql.Cursor
) -> tuple[FetchResult, Optional[LWBeatmap]]:
    """Attempts to fetch the beatmap from multiple sources, ordered by
    speed. Handles updates and ensures the correct object is fetched."""

    if st := cache.no_check_md5s.get(md5):
        debug(f"The MD5 {md5} is on the no check list. It will not be fetched.")
        if st == Status.UPDATE_AVAILABLE:
            return (FetchResult.NONE, UPDATE_BMAP)
        else:
            return (FetchResult.NONE, NOT_SUB_BMAP)

    # Cache is fastest.
    bmap = LWBeatmap.from_cache(md5)
    res = 1

    if bmap is None:
        res += 1
        bmap = await LWBeatmap.from_db(md5, cur)
        if bmap is None:
            res += 1
            bmap = await LWBeatmap.from_akat_mirror(md5)

        if bmap is not None:
            await bmap.save(cur)
            bmap.cache()

    # If the beatmap was not found from all sources, give up.
    if not bmap:
        debug(f"Added {md5} to no check list. It will not be looked up.")
        cache.no_check_md5s[md5] = Status.NOT_SUBMITTED
        return (FetchResult.NONE, None)

    if bmap.status == Status.UPDATE_AVAILABLE:
        cache.no_check_md5s[md5] = Status.UPDATE_AVAILABLE
        debug(f"Added {md5} to no check list. It will not be looked up.")
        return (FetchResult.NONE, None)

    # Check if we need to try to update.
    if bmap.deserves_update:
        debug(f"Checking for updates for {bmap!r}")

        current_data = await LWBeatmap.from_akat_mirror(md5)
        if current_data:
            bmap.last_updated = datetime.now()

            if current_data.md5 != bmap.md5:
                debug(f"Updating {bmap!r}")
                bmap = current_data

                bmap.cache()
                await bmap.save(cur)

                res += 1

    return FetchResult(res), bmap
