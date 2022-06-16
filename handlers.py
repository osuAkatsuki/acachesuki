import time
import copy
import base64
from urllib.parse import unquote, unquote_plus

from starlette.requests import Request
from starlette.responses import Response, PlainTextResponse
from const import FetchResult, LeaderboardTypes, MapResult, Mode
from globs import cache
from globs.conn import conns
from logger import info, error
from objects.beatmap import LWBeatmap
from objects.leaderboards import MAX_SCORES, LeaderboardResult, PersonalBestResult
from objects.score import Score
from pathlib import Path
from config import conf
from aiohttp import ClientSession
from helpers.user import restrict_user


def __format_score(score: tuple, place: int, get_clans: bool = True) -> str:
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


def __beatmap_header(bmap: LWBeatmap, score_count: int = 0) -> str:
    """Creates a response header for a beatmap."""

    if not bmap.has_leaderboard:
        return f"{bmap.status.value}|false"

    return (
        f"{bmap.status.value}|false|{bmap.id}|{bmap.set_id}|{score_count}\n"
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

    relax = mods & 128 > 0 and mode_int != 3
    mode = Mode.from_mode_int(mode_int, relax)

    safe_name = username.rstrip().lower().replace(" ", "_")

    # Acquire conn.
    async with conns.sql.acquire() as conn:
        async with conn.cursor() as cur:
            # Handle authentication.
            user_id = await cache.password.check_user(safe_name, password, cur)

            if not user_id:
                info(f"Received incorrect username + password combo from {username}.")
                return PlainTextResponse("error: pass")

            country = cache.country.get(user_id)
            if country is None:  # can still be "false" in the case of an empty list
                country = await cache.country.cache_individual(user_id, cur)

            friends_list = cache.friends.get(user_id)
            if (
                friends_list is None
            ):  # can still be "false" in the case of an empty list
                friends_list = await cache.friends.cache_individual(user_id, cur)

            lb = await LeaderboardResult.from_md5(
                cur,
                md5,
                mode,
                lb_type,
                unquote_plus(request.query_params["f"]),
                mods,
                country,
                friends_list,
            )

            if lb is MapResult.UNSUBMITTED:
                return PlainTextResponse("-1|false")
            elif lb is MapResult.UPDATE_REQUIRED:
                return PlainTextResponse("1|false")
            elif lb.bmap is None:
                return PlainTextResponse("-1|false")
            elif not lb.bmap.has_leaderboard:
                return PlainTextResponse(f"{lb.bmap.status}|false")

            pb_data = None
            if lb.scores:
                # Personal best calculation.
                pb_data = cache.personal_best.get((lb.bmap.md5, user_id, mode))
                pb_fetch = 1

                # See if we can use our prev result.
                if not pb_data:
                    score_data = lb.fetch_score(user_id)
                    if score_data:
                        # Create object from it.
                        pb_data = PersonalBestResult(
                            score_data[0],
                            score_data[1],
                            user_id,
                            lb.bmap.md5,
                            mode,
                        )
                    elif (not pb_data) and lb.total_scores > MAX_SCORES:
                        pb_fetch += 1
                        # Grab PB.
                        scoring = "pp" if mode.relax else "score"
                        table = "scores" if not mode.relax else "scores_relax"
                        await cur.execute(
                            PB_QUERY.format(scoring=scoring, table=table),
                            (user_id, lb.bmap.md5, mode.as_mode_int()),
                        )
                        pb_db = await cur.fetchone()
                        if pb_db:
                            # We need placement.
                            await cur.execute(
                                PB_PLACEMENT.format(table=table, scoring=scoring),
                                (pb_db[1], lb.bmap.md5, mode.as_mode_int()),
                            )
                            placement = (await cur.fetchone())[0]

                            # Finally, create the object.
                            pb_data = PersonalBestResult(
                                placement=placement,
                                score=pb_db,
                                user_id=user_id,
                                bmap_md5=lb.bmap.md5,
                                mode=mode,
                            )

            # If we still dont have pb, the user doesnt have one on this map.
            # Cache an empty pb object.
            if lb.scores and not pb_data:
                pb_fetch = 0
                pb_data = PersonalBestResult(
                    placement=0, score=None, user_id=user_id, bmap_md5=md5, mode=mode
                )
            if pb_data:
                pb_data.cache()
            else:
                pb_fetch = 0
            pb_fetch = FetchResult(pb_fetch)

            user_priv = cache.priv.get(user_id)
            if not user_priv:
                user_priv = await cache.priv.cache_individual(user_id, cur)

    if user_priv & 4:  # donor
        score_limit = lb.scores[:250]
    elif user_priv & 8388608:  # premium
        score_limit = lb.scores[:500]
    else:  # normal user
        score_limit = lb.scores[:150]

    # Build final response.
    resp = "\n".join(
        [
            __beatmap_header(lb.bmap, lb.total_scores),
            __format_score(pb_data.score, pb_data.placement, False)
            if pb_data and pb_data.score
            else "",
            *(
                [
                    __format_score(s, idx + 1, s[13] != user_id)
                    for idx, s in enumerate(score_limit)
                ]
                if lb.bmap.status.has_lb
                else []
            ),
        ]
    ).encode()

    time_taken = (time.time() - start_time) * 1000
    info(
        f"Beatmap {lb.bmap_fetch.console_text} / Leaderboard {lb.lb_fetch.console_text} / PB {pb_fetch.console_text} |"
        f" Served '{username}' ({user_id}) leaderboard for {lb.bmap.song_name} ({time_taken:.2f}ms)"
    )

    # Cache result for later.
    lb.cache()

    return PlainTextResponse(resp)


UNRANKED_MODS = (
    1 << 29 | 1 << 11 | 1 << 23 | 1 << 13
)  # score v2, auto, target, autopilot


def pair_panel(name, before_val, after_val) -> str:
    """Function to create a paired panel string for use in score submission"""

    return f"{name}Before:{before_val or ''}|{name}After:{after_val}"  # peppy why


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

            table = "scores" if not score.mode.relax else "scores_relax"
            await cur.execute(
                f"SELECT 1 FROM {table} WHERE checksum = %s", (score.checksum,)
            )

            if await cur.fetchone():
                score.status = -1  # duplicate
                return PlainTextResponse("error: no")

            elapsed = args["st" if score.passed else "ft"]
            # if not elapsed or not elapsed.isdecimal():
            #    await restrict_user(score.user_id, "Invalid time (old/edited client)")

            # pp caps
            if (
                score.passed
                and score.map.gives_pp
                and not cache.whitelist.get(score.user_id, score.mode.relax)
                and cache.priv.get(score.user_id) & 1
            ):
                if score.mode.relax:
                    cap = "rx"
                else:
                    cap = "vn"

                if score.mods & 1 << 10:
                    cap += "fl"

                pp_cap = cache.pp_caps[score.mode.as_mode_int()][cap]

                if int(score.pp) >= pp_cap:
                    await restrict_user(
                        score.user_id,
                        f"Restricted for surpassing {'Relax' if score.mode.relax else 'Vanilla'} pp cap ({score.pp:.2f}pp)",
                    )

            await score.submit(cur)
            if (
                score.passed
                and cache.priv.get(score.user_id) & 1
                and score.map.has_leaderboard
            ):  # restricted & ranked check
                await score.calc_rank(cur)
            else:
                score.rank = 0

            if score.passed:
                replay = await args.getlist("score")[1].read()
                # print(replay)
                if not replay or replay == b"\r\n":
                    await restrict_user(
                        score.user_id, "Restricted for missing/invalid replay file"
                    )

                async with ClientSession() as session:
                    await session.post(
                        f"http://localhost:8484/save?id={score.id}", data=replay
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

            additive = score.score
            if score.previous_score and score.status == 3:
                additive -= score.previous_score.score

            if score.passed and score.map.has_leaderboard:
                if score.map.status == 2:
                    stats.ranked_score += additive
                if score.status == 3 and score.pp:
                    await stats.recalc(cur)

            await stats.save(cur)

        # Create beatmap info panel.
        panels = []
        new_achievements = []  # TODO

        await stats.refresh_stats()
        if score.passed and old_stats.pp != stats.pp:
            await stats.update_rank()

        if score.status == 3:
            cache.leaderboard.remove_lb_cache(
                (
                    score.mode,
                    score.map.md5,
                )
            )
            cache.personal_best.remove_cache((score.map.md5, score.user_id, score.mode))

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
                            pair_panel("maxCombo", "0", "0"),  # TODO: add to db
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

            table = "scores_relax" if replay_id < 500000000 else "scores"
            table_stats = "rx_stats" if replay_id < 500000000 else "users_stats"
            await cur.execute(
                f"SELECT t.play_mode, u.username FROM {table} t LEFT JOIN users u ON t.userid = u.id WHERE t.id = %s",
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
                    [user_id],
                )

            async with ClientSession() as session:
                async with session.get(
                    f"http://localhost:8484/get?id={replay_id}"
                ) as session:
                    if not session or session.status != 200:
                        return PlainTextResponse("")

                    return Response(await session.read())
