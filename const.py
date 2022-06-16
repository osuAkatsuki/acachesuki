from enum import IntEnum
from typing import Any, Callable, Optional, TypeVar

import pymysql
from colorama import Fore

import re

T = TypeVar("T")


def pymysql_encode(
    conv: Callable[[Any, Optional[dict[object, object]]], str]
) -> Callable[[T], T]:
    """Decorator to allow for adding to pymysql's encoders."""

    def wrapper(cls: T) -> T:
        pymysql.converters.encoders[cls] = conv
        return cls

    return wrapper


def escape_enum(
    val: Any, _: Optional[dict[object, object]] = None
) -> str:  # used for ^
    return str(int(val))


@pymysql_encode(escape_enum)
class Status(IntEnum):
    """An enum of beatmap statuses."""

    GRAVEYARD = -2
    NOT_SUBMITTED = -1
    PENDING = 0
    UPDATE_AVAILABLE = 1
    RANKED = 2
    APPROVED = 3
    QUALIFIED = 4
    LOVED = 5

    @property
    def has_lb(self) -> bool:
        """Property corresponding to whether the status should offer leaderboards."""

        return self in LB_STATUSES

    @classmethod
    def from_api(cls, status: int) -> "Status":
        """Converts an osu!api status enum to a regular status enum."""

        if status <= 0:
            return Status.PENDING
        return Status(status + 1)


LB_STATUSES = (Status.LOVED, Status.QUALIFIED, Status.APPROVED, Status.RANKED)


class LeaderboardTypes(IntEnum):
    """osu! in-game leaderboards types."""

    LOCAL = 0  # Not used online.
    TOP = 1  # Regular top leaderboards.
    MOD = 2  # Leaderboards for a specific mod combo.
    FRIENDS = 3  # Leaderboard containing only the user's friends.
    COUNTRY = 4  # Leaderboards containing only people from the user's nation.


@pymysql_encode(escape_enum)
class Mode(IntEnum):
    VN_STANDARD = 0
    VN_TAIKO = 1
    VN_CATCH = 2
    VN_MANIA = 3

    # RELAX
    RX_STANDARD = 4
    RX_TAIKO = 5
    RX_CATCH = 6

    # AUTOPILOT
    AP_STANDARD = 7

    def as_mode_int(self) -> int:
        """Converts the mode enum to a mode int for storage in the db."""

        val = self.value
        if val == 7:
            return 0
        if val > 3:
            val -= 4
        return val

    @property
    def relax(self) -> bool:
        """Property stating whether the mode is a relax mode."""

        return self.value > 3 and self.value != 7

    @property
    def autopilot(self) -> bool:
        return self.value == 7

    @property
    def db_prefix(self) -> str:
        """Property stating the prefix for the mode in the db."""

        mode_int = self.as_mode_int()

        if mode_int == 0:
            return "std"
        elif mode_int == 1:
            return "taiko"
        elif mode_int == 2:
            return "ctb"
        elif mode_int == 3:
            return "mania"

    @property
    def annouce_prefix(self) -> str:
        mode_int = self.as_mode_int()

        if mode_int == 0:
            return "osu!"
        elif mode_int == 1:
            return "Taiko"
        elif mode_int == 2:
            return "Catch"
        elif mode_int == 3:
            return "Mania"
        else:
            raise NotImplementedError(f"Unknown mode int: {mode_int}")

    @staticmethod
    def from_mode_int(mode: int, mods: int) -> "Mode":
        """Converts a mode int and presence of rx/ap into a `Mode` enum."""

        if mode == 3:
            return Mode.VN_MANIA
        if mods & 128:
            mode += 4
        if mods & 8192:
            return Mode.AP_STANDARD
        return Mode(mode)

    @property
    def stats_table(self) -> str:
        if self.autopilot:
            return "ap_stats"

        if self.relax:
            return "rx_stats"

        return "users_stats"

    @property
    def scores_table(self) -> str:
        if self.autopilot:
            return "scores_ap"

        if self.relax:
            return "scores_relax"

        return "scores"

    @property
    def leaderboard_str(self) -> str:
        if self.autopilot:
            return "autoboard"

        if self.relax:
            return "relaxboard"

        return "leaderboard"


FETCH_COL = (
    Fore.RED,  # None
    Fore.GREEN,  # Cache
    Fore.BLUE,  # MySQL
    Fore.YELLOW,  # API
)

FETCH_TEXT = ("No Result", "Cache", "MySQL", "API")


class FetchResult(IntEnum):
    """Internal enum representing how a resource was fetched. Made mostly
    for logging purposes."""

    NONE = 0  # No result
    CACHE = 1
    MYSQL = 2
    API = 3

    @property
    def result_exists(self) -> bool:
        """Whether the fetch result value means there is a valid result present."""

        return self.value > 0

    @property
    def colour(self) -> str:
        """Returns the colorama colour that should be used for the status."""

        return FETCH_COL[self.value]

    @property
    def console_text(self) -> str:
        """Returns the text string to be used in loggign."""

        return f"{self.colour}{FETCH_TEXT[self.value]}{Fore.RESET}{Fore.WHITE}"


MAP_FILENAME = re.compile(
    r"^(?P<artist>.+) - (?P<title>.+) \((?P<mapper>.+)\) \[(?P<diff>.+)\]\.osu$"
)


class MapResult(IntEnum):
    """Internal enum representing how a map was fetched. Made mostly
    for logging purposes."""

    UNSUBMITTED = 0  # No result
    UPDATE_REQUIRED = 1  # Exists, not up to date on the client's end
    EXISTS = 2  # No issues, unused
