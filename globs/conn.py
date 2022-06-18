import traceback
from dataclasses import dataclass
from dataclasses import field

import aiomysql
import aioredis

from config import conf
from logger import error
from logger import info


# Big botch.
@dataclass
class Connections:
    sql: aiomysql.Pool = field(init=False)
    redis: aioredis.Redis = field(init=False)

    async def establish(self) -> None:
        """Establishes all the required connections."""

        self.sql = await create_sql_pool()
        self.redis = await create_redis_pool()


async def create_redis_pool() -> aioredis.Redis:
    """Creates a connection to the redis server."""

    info("Attempting to connect to redis @ redis://localhost")

    try:
        conn = aioredis.Redis(await aioredis.create_pool("redis://localhost"))
        info("Successfully connected to Redis!")
        return conn
    except Exception:
        error("Failed connecting to Redis with error " + traceback.format_exc())
        raise SystemExit(1)


async def create_sql_pool() -> aiomysql.Pool:
    """Creates the connection to MySQL."""

    info(f"Attempting to connect to MySQL ({conf.sql_host}:3306 @ {conf.sql_db})")
    try:
        pool = await aiomysql.create_pool(
            host=conf.sql_host,
            port=3306,
            user=conf.sql_user,
            password=conf.sql_password,
            db=conf.sql_db,
            pool_recycle=False,
            autocommit=True,
        )
        info("Successfully connected to the database!")
        return pool
    except Exception:
        error("Failed connecting to MySQL with error " + traceback.format_exc())
        raise SystemExit(1)


conns = Connections()
