#!/usr/bin/env python3.9

from config import conf
import uvicorn
from globs.cache import init_caches
from globs.conn import conns
from starlette.applications import Starlette
from starlette.routing import Route
from logger import debug, error, info, DEBUG
from pubsubs import (
    # handle_restricted_user,
    pubsub_executor,
    handle_status_update,
    # name_change,
)
from handlers import handle_submission, handle_replays, handle_leaderboards

# uvloop is a significantly faster loop.
try:
    import uvloop

    uvloop.install()
except ImportError:
    error("Not using uvloop! Performance may be degraded.")

__version__ = "0.0.2"

# TODO: name updates, score wipes
REDIS_PUBSUB = (
    ("cache:map_update", handle_status_update),
    # ("peppy:ban", handle_restricted_user),
    # ("peppy:change_username", name_change),
)


async def create_pubsub_listeners() -> None:
    """Creates listeners for redis pub events."""

    for name, handler in REDIS_PUBSUB:
        await pubsub_executor(name, handler)
        debug(f"Subscribed to Redis event {name}")

    info(f"Created {len(REDIS_PUBSUB)} Redis Listeners.")


PRERUN_TASKS = (conns.establish, init_caches, create_pubsub_listeners)


async def execute_all_tasks() -> None:
    """Runs all of the pre-run tasks."""
    for task in PRERUN_TASKS:
        await task()


def main() -> int:
    info(f"Acachesuki {__version__} is starting...")

    app = Starlette(
        debug=DEBUG,
        on_startup=[execute_all_tasks],
        routes=[
            Route(
                "/web/osu-submit-modular-selector.php",
                handle_submission,
                methods=["POST"],
            ),
            Route("/web/osu-getreplay.php", handle_replays),
            Route("/web/osu-osz2-getscores.php", handle_leaderboards),
        ],
    )
    uvicorn.run(app, uds=conf.http_sock, access_log=False)
    # TODO: may be worth tinkering further with gzip level
    # app = Server(name="Acachesuki", max_conns=15, gzip=7)

    # # connect to sql database
    # app.add_pending_task(execute_all_tasks())

    # # add routes being managed by the server
    # from handlers import dom as osu_akatsuki_pw

    # app.add_domain(osu_akatsuki_pw)

    # # run the server indefinitely
    # app.run(conf.http_sock)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
