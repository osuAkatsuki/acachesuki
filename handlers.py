import time
import copy
import base64
from urllib.parse import unquote, unquote_plus
import aiohttp
import aiomysql

from starlette.requests import Request
from starlette.responses import Response, PlainTextResponse
from const import FetchResult, LeaderboardTypes, Mode, Status
from globs import cache
from globs.conn import conns
from logger import info
from objects.beatmap import LWBeatmap, try_bmap
from objects.leaderboards import Leaderboard
from objects.score import Score
from aiohttp import ClientSession
from helpers.user import restrict_user


def __format_score_old(score: tuple, place: int, get_clans: bool = True) -> str:
    """Formats a Database score tuple into a string format understood by the
    client."""

    name = score[12]
    if get_clans:
        if clan := cache.clan.get(score[13]):
            name = f"[{clan}] " + name

    return (
        f"{score[0]}|{name}|{score[1]:.0f}|{score[2]}|{score[3]}|"
        f"{score[4]}|{score[5]}|{score[6]}|{score[7]}|{score[8]}|"
        f"{score[9]}|{score[10]}|{score[13]}|{place}|{score[11]}|"
        "1"
    )


def __format_score(
    score: Score, rank: int, username: str, get_clans: bool = True
) -> str:
    if get_clans:
        if clan := cache.clan.get(score.user_id):
            username = f"[{clan}] " + username

    if score.mode > Mode.VN_MANIA:
        displayed_score = int(score.pp)
    else:
        displayed_score = score.score

    return (
        f"{score.id}|{username}|{displayed_score}|{score.combo}|{score.n50}|{score.n100}|{score.n300}|{score.miss}|"
        f"{score.katu}|{score.geki}|{int(score.fc)}|{int(score.mods)}|{score.user_id}|{rank}|{score.time}|"
        "1"  # has replay
    )


async def __format_score_reg(
    cur: aiomysql.Cursor, score: Score, rank: int, get_clans: bool = True
):
    await cur.execute("SELECT username FROM users WHERE id = %s", (score.user_id,))
    username = (await cur.fetchone())[0]

    return __format_score(score, rank, username, get_clans)


def __beatmap_header(bmap: LWBeatmap, score_count: int = 0) -> str:
    """Creates a response header for a beatmap."""

    if not bmap.has_leaderboard:
        return f"{bmap.status.value}|false"

    return (
        f"{bmap.status.value}|false|{bmap.id}|{bmap.set_id}|{score_count}|0||\n"  # 0 is featured artist track id, || is empty string for license text
        f"0\n{bmap.song_name}\n{bmap.rating}"
    )


def error_score(msg: str) -> str:
    """Generates an error message as a score from Aika."""

    return f"999|{msg}|999999999|0|0|0|0|0|0|0|0|0|999|0|0|1"


def maintenence_lbs() -> str:
    """Shows leaderboards as under maintenence."""

    return "2|false\n\n\n\n\n" + "\n".join(
        [
            error_score("Leaderboard Maintenence"),
            error_score("Score Submit still works"),
        ]
    )


def error_lbs(msg: str) -> str:
    """Displays an error to the user in a visual manner."""

    return "2|false\n\n\n\n\n" + "\n".join(
        [error_score("Leaderboard Error!"), error_score(msg)]
    )


PB_QUERY = """
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
    s.pp
FROM
    {table} s
INNER JOIN
    users a on s.userid = a.id
WHERE
    a.id = %s AND
    s.beatmap_md5 = %s AND
    s.play_mode = %s AND
    s.completed = 3
LIMIT 1
"""

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


async def handle_leaderboards(request: Request) -> Response:
    """Handles the leaderboard endpoint."""

    start_time = time.time()  # Performance metrics

    username = unquote(request.query_params["us"])
    password = request.query_params["ha"]
    mods = int(request.query_params["mods"])
    md5 = request.query_params["c"]
    mode_int = int(request.query_params["m"])
    lb_type = LeaderboardTypes(int(request.query_params["v"]))

    mode = Mode.from_mode_int(mode_int, mods)

    safe_name = username.rstrip().lower().replace(" ", "_")

    # Acquire conn.
    async with conns.sql.acquire() as conn:
        async with conn.cursor() as cur:
            # Handle authentication.
            user_id = await cache.password.check_user(safe_name, password, cur)

            if not user_id:
                info(f"Received incorrect username + password combo from {username}.")
                return PlainTextResponse("error: pass")

            await cur.execute(
                "SELECT country FROM users_stats WHERE id = %s",
                (user_id,),
            )

            country_db = await cur.fetchone()
            if not country_db:
                country = "XX"
            else:
                country = country_db[0]

            await cur.execute(
                "SELECT user2 FROM users_relationships WHERE user1 = %s",
                (user_id,),
            )

            f_db = await cur.fetchall()
            friends_list = [uid for uid in f_db] + [user_id]

            result, beatmap = await try_bmap(md5, cur)
            if result == FetchResult.NONE:
                return PlainTextResponse("-1|false")

            assert beatmap is not None

            if beatmap.status is Status.NOT_SUBMITTED:
                return PlainTextResponse("-1|false")
            elif beatmap.status is Status.UPDATE_AVAILABLE:
                return PlainTextResponse("1|false")
            elif not beatmap.has_leaderboard:
                return PlainTextResponse(f"{beatmap.status}|false")

            lb = await Leaderboard.get_leaderboard(
                beatmap, cur, unquote_plus(request.query_params["f"]), mode
            )
            if lb is None:
                return PlainTextResponse("-1|false")

            personal_best = lb.find_user_score(user_id)
            personal_fetch = FetchResult.CACHE
            if personal_best is None:
                personal_fetch = FetchResult.NONE

            user_priv = cache.priv.get(user_id)
            if user_priv is None:
                user_priv = await cache.priv.cache_individual(user_id, cur)
                assert user_priv is not None

            scores: list[Score] = []

            if user_priv & 4:  # donor
                score_limit = 250
            elif user_priv & 8388608:  # premium
                score_limit = 500
            else:  # normal user
                score_limit = 150

            for score in lb.scores:
                if len(scores) >= score_limit:
                    break

                await cur.execute(
                    "SELECT privileges FROM users WHERE id = %s", (score.user_id,)
                )
                user_priv = (await cur.fetchone())[0]

                await cur.execute(
                    "SELECT country FROM users_stats WHERE id = %s", (score.user_id,)
                )
                user_country = (await cur.fetchone())[0]

                if not user_priv & 1 and score.user_id != user_id:
                    continue

                if lb_type == LeaderboardTypes.MOD and score.mods != mods:
                    continue

                if lb_type == LeaderboardTypes.COUNTRY and user_country != country:
                    continue

                if (
                    lb_type == LeaderboardTypes.FRIENDS
                    and score.user_id not in friends_list
                ):
                    continue

                scores.append(score)

            scoring = "pp" if mode > Mode.VN_MANIA else "score"
            if personal_best:
                for idx, score in enumerate(scores):
                    if getattr(score, scoring) < getattr(
                        personal_best["score"], scoring
                    ):
                        personal_best["rank"] = idx
                        break

            # Build final response.
            resp = "\n".join(
                [
                    __beatmap_header(beatmap, len(lb)),
                    __format_score(
                        personal_best["score"], personal_best["rank"], username, False
                    )
                    if personal_best
                    else "",
                    *(
                        [
                            await __format_score_reg(
                                cur, score, idx + 1, score.user_id != user_id
                            )
                            for idx, score in enumerate(scores)
                        ]
                        if beatmap.status.has_lb
                        else []
                    ),
                ]
            ).encode()

    time_taken = (time.time() - start_time) * 1000
    info(
        f"Beatmap {result.console_text} / Leaderboard {lb.lb_fetch.console_text} / PB {personal_fetch.console_text} |"
        f" Served '{username}' ({user_id}) leaderboard for {beatmap.song_name} ({time_taken:.2f}ms)"
    )

    return PlainTextResponse(resp)


UNRANKED_MODS = 1 << 29 | 1 << 11 | 1 << 23  # score v2, auto, target


def pair_panel(name, before_val, after_val) -> str:
    """Function to create a paired panel string for use in score submission"""

    return f"{name}Before:{before_val or ''}|{name}After:{after_val}"  # peppy why


from osupyparser.osr.osr_parser import ReplayFile
from osupyparser.osr.constants import OsuReplayFrame

from osupyparser.osu.osu_parser import OsuFile
from osupyparser.osu.objects import HitObject, Slider, Spinner
from dataclasses import dataclass


def is_straight(x: list[float], y: list[float]) -> bool:
    threshold = 1.0
    is_straight_line = True

    x = list(set([i for i in x if i > 0.0]))
    y = list(set([i for i in y if i > 0.0]))

    slope = (y[1] - y[0]) / (x[1] - x[0])

    for i, (xval, yval) in enumerate(zip(x[2:], y[2:])):
        s = (yval - y[i - 1]) / (xval - x[i - 1])

        if abs(s - slope) > threshold:
            is_straight_line = False
            break

    return is_straight_line


@dataclass
class Play:
    replay: ReplayFile
    map: OsuFile

    def get_movements_between_objects(
        self,
        object1: HitObject,
        object2: HitObject,
    ) -> list[OsuReplayFrame]:
        earliest_time = object1.start_time
        latest_time = object2.start_time

        frames: list[OsuReplayFrame] = []

        for idx, frame in enumerate(self.replay.frames[:1000]):
            previous_frames = self.replay.frames[:idx] if idx > 0 else [frame]

            frame_time = sum(prev.delta for prev in previous_frames) + frame.delta
            if latest_time >= frame_time >= earliest_time:
                frames.append(frame)

        return frames

    @property
    def is_chattered(self) -> bool:
        is_chatter = False

        for idx, object in enumerate(self.map.hit_objects):
            if isinstance(object, (Slider, Spinner)):
                continue

            next_object = self.map.hit_objects[idx + 1]
            movements = self.get_movements_between_objects(object, next_object)

            x_movements = [movement.x for movement in movements]
            y_movements = [movement.y for movement in movements]

            is_chatter = not is_straight(x_movements, y_movements)
            break

        return is_chatter


async def chatter_check(replay_id: int, scores_table: str) -> None:
    """Automatically detects if a replay is using chatter plugin"""

    async with conns.sql.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(
                f"SELECT * FROM {scores_table} WHERE id = %s", (replay_id,)
            )
            score = await cur.fetchone()

            await cur.execute("SELECT * FROM users WHERE id = %s", (score["userid"],))
            user = await cur.fetchone()

            await cur.execute(
                "SELECT * FROM beatmaps WHERE beatmap_md5 = %s", (score["beatmap_md5"],)
            )
            bmap = await cur.fetchone()

    async with ClientSession() as session:
        async with session.get(f"http://localhost:8484/get?id={replay_id}") as session:
            if not session or session.status != 200:
                return

            replay_bytes = await session.read()

    replay = ReplayFile.from_bytes(replay_bytes, pure_lzma=True)
    osu_file = OsuFile(f"/home/akatsuki/lets/.data/beatmaps/{bmap['beatmap_id']}.osu")
    osu_file.parse_file()

    play = Play(replay, osu_file)

    if play.is_chattered:
        replay_url = f"https://akatsuki.pw/web/replays/{replay_id}"
        await restrict_user(
            user["id"],
            f"Restricted by auto chatter detection. Please check this replay: {replay_url}",
        )


async def handle_submission(request: Request) -> Response:
    """Handles the submission endpoint."""

    args, headers = await request.form(), request.headers
    # args, headers = request.multipart_args, request.headers

    async with conns.sql.acquire() as conn:
        async with conn.cursor() as cur:
            score = await Score.from_score_submission(args, cur)

            if not score:
                return PlainTextResponse("error: no")  # error within score sub
            elif not score.user_id:
                return PlainTextResponse("")  # user isn't online
            elif not score.map:
                return PlainTextResponse("error: beatmap")  # no map
            elif score.mods & UNRANKED_MODS:
                return PlainTextResponse("error: no")

            if not headers.get("Token"):
                await restrict_user(
                    score.user_id, "Restricted for missing token header"
                )

            fs_data = args["fs"]
            try:
                decoded = base64.b64decode(fs_data).decode(errors="ignore")

                if (
                    decoded[8] == "-"
                    and decoded[13] == "-"
                    and decoded[18] == "-"
                    and decoded[23] == "-"
                    and len(decoded) == 36
                ):
                    score.using_patcher = True
                else:
                    score.using_patcher = False
            except Exception:
                score.using_patcher = False

            if headers.get("User-Agent") != "osu!":
                await restrict_user(score.user_id, "Restricted for User-Agent != osu!")

                fl_screenshot_file = args.get("i")
                if fl_screenshot_file:
                    await restrict_user(
                        score.user_id, "Restricted for 'i' screenshot file on score sub"
                    )

            table = score.mode.scores_table
            await cur.execute(
                f"SELECT 1 FROM {table} WHERE checksum = %s", (score.checksum,)
            )

            if await cur.fetchone():
                score.status = -1  # duplicate
                return PlainTextResponse("error: no")
            
            if score.passed:
                await cur.execute(
                    f"SELECT id FROM {table} WHERE userid = %s AND "
                    f"beatmap_md5 = %s AND completed = 3 AND play_mode = {score.mode.as_mode_int()} LIMIT 1",
                    (score.user_id, score.map.md5)
                )
                prev_db = await cur.fetchone()
                
                score.previous_score = await Score.from_id(
                    prev_db[0], table, cur
                ) if prev_db else None

            elapsed = args["st" if score.passed else "ft"]
            # if not elapsed or not elapsed.isdecimal():
            #    await restrict_user(score.user_id, "Invalid time (old/edited client)")

            # pp caps
            if (
                score.passed
                and score.map.gives_pp
                and not cache.whitelist.get(
                    score.user_id, score.mode.relax or score.mode.autopilot
                )  # TODO: sep rx/ap whitelist
                and cache.priv.get(score.user_id) & 1
            ):
                if score.mode.relax:
                    cap = "rx"
                elif score.mode.autopilot:
                    cap = "ap"
                else:
                    cap = "vn"

                if score.mods & 1 << 10:
                    cap += "fl"

                pp_cap = cache.pp_caps[score.mode.as_mode_int()][cap]

                if int(score.pp) >= pp_cap:
                    await restrict_user(
                        score.user_id,
                        f"Restricted for surpassing {'Relax' if score.mode.relax else ('Vanilla' if not score.mode.autopilot else 'Autopilot')} pp cap ({score.pp:.2f}pp)",
                    )

            leaderboard = await Leaderboard.get_leaderboard(
                score.map, cur, "", score.mode
            )
            previous_score = leaderboard.find_user_score(score.user_id)
            if previous_score:
                score.previous_score = previous_score["score"]
                score.previous_score.rank = previous_score["rank"]

            await score.submit(cur)

            # update most played beatmaps
            await cur.execute(
                "INSERT INTO user_beatmaps (userid, map, rx, mode, count) VALUES (%s, %s, %s, %s, 1) ON DUPLICATE KEY UPDATE count = count + 1",
                (
                    score.user_id,
                    score.map.md5,
                    1 if score.mode.relax else (2 if score.mode.autopilot else 0),
                    score.mode.as_mode_int(),
                ),
            )

            if score.passed:
                replay = await args.getlist("score")[1].read()
                # print(replay)
                if not replay or replay == b"\r\n":
                    await restrict_user(
                        score.user_id, "Restricted for missing/invalid replay file"
                    )

                async with ClientSession(
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as session:
                    await session.post(
                        f"http://localhost:3030/save?id={score.id}", data=replay
                    )

            await score.map.increment_counts(cur)

            stats = await cache.stats.get(score.user_id, score.mode, cur)
            if not stats:
                info(f"{score.user_name} has no stats?")
                return PlainTextResponse("")  # try resubmission

            old_stats = copy.copy(stats)

            stats.playcount += 1
            stats.total_score += score.score
            stats.total_hits += score.n300 + score.n100 + score.n50

            if score.mode.as_mode_int() in (1, 3):
                stats.total_hits += score.geki + score.katu

            if score.passed and score.map.has_leaderboard:
                
                if score.status == 3 and score.map.status == 2:
                    score_to_add = score.score
                
                    if score.previous_score:
                        score_to_add -= score.previous_score.score
                    
                    stats.ranked_score += score_to_add
                
                if score.combo > stats.max_combo:
                    stats.max_combo = score.combo

                if score.status == 3 and score.pp:
                    await stats.recalc(cur)

            await stats.save(cur)

            if score.status == 3:
                leaderboard.add_score(score)
                score.rank = leaderboard.find_score_rank(score.id)
            elif score.status == 2:  # so hacky lol
                leaderboard.add_score(score)
                score.rank = leaderboard.find_score_rank(score.id)

                if score.previous_score:
                    leaderboard.add_score(score.previous_score)
                else:
                    leaderboard.remove_user(score.user_id)

            if (
                score.rank == 1
                and score.status == 3
                and cache.priv.get(score.user_id) & 1
                and score.map.has_leaderboard
            ):
                await score.first_place(cur)

        # Create beatmap info panel.
        panels = []
        new_achievements = []  # TODO

        await stats.refresh_stats()
        if (
            score.passed
            and old_stats.pp != stats.pp
            and cache.priv.get(score.user_id) & 1
        ):
            await stats.update_rank()

        panels.append(
            f"beatmapId:{score.map.id}|"
            f"beatmapSetId:{score.map.set_id}|"
            f"beatmapPlaycount:{score.map.playcount}|"
            f"beatmapPasscount:{score.map.passcount}|"
            f"approvedDate:{score.map.formatted_time}"
        )

        failed_not_prev_panel = (
            (
                pair_panel("rank", "0", score.rank),
                pair_panel("maxCombo", "", score.combo),
                pair_panel("accuracy", "", round(score.acc, 2)),
                pair_panel("rankedScore", "", score.score),
                pair_panel("pp", "", score.pp),
            )
            if score.passed
            else (
                pair_panel("rank", "0", "0"),
                pair_panel("maxCombo", "", score.combo),
                pair_panel("accuracy", "", ""),
                pair_panel("rankedScore", "", score.score),
                pair_panel("pp", "", ""),
            )
        )

        if score.map.has_leaderboard:
            # Beatmap ranking panel.
            panels.append(
                "|".join(
                    (
                        "chartId:beatmap",
                        f"chartUrl:{score.map.url}",
                        "chartName:Beatmap Ranking",
                        *(
                            failed_not_prev_panel
                            if not score.previous_score or not score.passed
                            else (
                                pair_panel(
                                    "rank", score.previous_score.rank, score.rank
                                ),
                                pair_panel(
                                    "maxCombo", score.previous_score.combo, score.combo
                                ),
                                pair_panel(
                                    "accuracy",
                                    round(score.previous_score.acc, 2),
                                    round(score.acc, 2),
                                ),
                                pair_panel(
                                    "rankedScore",
                                    score.previous_score.score,
                                    score.score,
                                ),
                                pair_panel(
                                    "pp",
                                    round(score.previous_score.pp),
                                    round(score.pp),
                                ),
                            )
                        ),
                        f"onlineScoreId:{score.id}",
                    )
                )
            )

        panels.append(
            "|".join(
                (
                    "chartId:overall",
                    f"chartUrl:https://akatsuki.pw/u/{score.user_id}",
                    "chartName:Global Ranking",
                    *(
                        (
                            pair_panel("rank", old_stats.rank, stats.rank),
                            pair_panel(
                                "rankedScore",
                                old_stats.ranked_score,
                                stats.ranked_score,
                            ),
                            pair_panel(
                                "totalScore", old_stats.total_score, stats.total_score
                            ),
                            pair_panel(
                                "maxCombo", old_stats.max_combo, stats.max_combo
                            ),
                            pair_panel(
                                "accuracy",
                                round(old_stats.accuracy, 2),
                                round(stats.accuracy, 2),
                            ),
                            pair_panel("pp", round(old_stats.pp), round(stats.pp)),
                        )
                    ),
                    f"achievements-new:{'/'.join(new_achievements)}",
                    f"onlineScoreId:{score.id}",
                )
            )
        )

        info(
            f"{score.user_name} submitted a {score.pp:,.2f}pp score on {score.map!r} ({score.mode.name} | id: {score.id} | completed: {score.status})"
        )

        # if score.mode.as_mode_int() == 0 and score.status > 1:
        # asyncio.ensure_future(chatter_check(score.id, table))

        return PlainTextResponse("\n".join(panels))


async def handle_replays(request: Request) -> Response:
    """Handles replays."""
    username = unquote(request.query_params["u"])
    password = request.query_params["h"]
    replay_id = int(request.query_params["c"])

    safe_name = username.rstrip().lower().replace(" ", "_")

    async with conns.sql.acquire() as conn:
        async with conn.cursor() as cur:
            # Handle authentication.
            user_id = await cache.password.check_user(safe_name, password, cur)

            if not user_id:
                info(f"Received incorrect username + password combo from {username}.")
                return PlainTextResponse("error: pass")

            table = "scores"

            if replay_id >= 6148914691236517204:
                table = "scores_ap"
            elif replay_id < 500000000:
                table = "scores_relax"

            table_stats = "users_stats"

            if table == "scores_ap":
                table_stats = "ap_stats"
            elif table == "scores_relax":
                table_stats = "rx_stats"

            await cur.execute(
                f"SELECT t.play_mode, u.username, u.id FROM {table} t LEFT JOIN users u ON t.userid = u.id WHERE t.id = %s",
                [replay_id],
            )
            replay_data = await cur.fetchone()

            if replay_data and username != replay_data[1]:
                suffix = {0: "std", 1: "taiko", 2: "ctb", 3: "mania"}.get(
                    replay_data[0], "std"
                )
                await cur.execute(
                    (
                        "UPDATE {table} SET replays_watched_{suff} = replays_watched_{suff} + 1 "
                        "WHERE id = %s LIMIT 1"
                    ).format(suff=suffix, table=table_stats),
                    [replay_data[2]],
                )

            async with ClientSession() as session:
                async with session.get(
                    f"http://localhost:3030/get?id={replay_id}"
                ) as session:
                    if not session or session.status != 200:
                        return PlainTextResponse("")

                    return Response(await session.read())
