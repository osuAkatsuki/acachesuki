from rosu_pp_py import Calculator as RCalculator, ScoreParams
from peace_performance_python import Calculator, Beatmap
from cmyui.osu.oppai_ng import OppaiWrapper
from aiohttp import ClientSession
from pathlib import Path

import math


class RosuCalculator:  # wrapper around peace performance for ease of use
    def __init__(self, score) -> None:
        self.score = score
        self.map = score.map

    def calculate(self, map_path: str) -> tuple[float]:
        calculator = RCalculator(map_path)
        params = ScoreParams(
            acc=self.score.acc,
            nMisses=self.score.miss,
            score=self.score.score,
            combo=self.score.combo,
            mods=self.score.mods,
        )

        (result,) = calculator.calculate(params)
        return (result.pp, result.stars)


class PeaceCalculator:
    def __init__(self, score) -> None:
        self.score = score
        self.map = score.map

    def calculate(self, map_path: str) -> tuple[float]:
        beatmap = Beatmap(map_path)
        calculator = Calculator(
            acc=self.score.acc,
            miss=self.score.miss,
            score=self.score.score,
            combo=self.score.combo,
            mode=self.score.mode.as_mode_int(),
            mods=self.score.mods,
        )

        result = calculator.calculate(beatmap)
        return (result.pp, result.stars)


class OppaiCalculator:  # wrapper around oppaiwrapper for ease of use
    def __init__(self, score) -> None:
        self.score = score
        self.map = score.map

    def calculate(self, map_path: str) -> tuple[float]:
        with OppaiWrapper("oppai-ng/liboppai.so") as calculator:
            calculator.configure(
                mode=self.score.mode.as_mode_int(),
                acc=self.score.acc,
                mods=self.score.mods,
                combo=self.score.combo,
                nmiss=self.score.miss,
            )

            calculator.calculate(map_path)

            pp, sr = calculator.get_pp(), calculator.get_sr()
            return (pp, sr)


class PPUtils:
    def __init__(self, score, calc) -> None:
        self.score = score
        self.map = score.map

        self.calc: object = calc

    @staticmethod
    def calc_peace(score) -> "PPUtils":
        self = PPUtils(score=score, calc=PeaceCalculator)

        return self

    @staticmethod
    def calc_oppai(score) -> "PPUtils":
        self = PPUtils(score=score, calc=OppaiCalculator)

        return self

    @staticmethod
    def calc_rosu(score) -> "PPUtils":
        self = PPUtils(score=score, calc=RosuCalculator)

        return self

    async def calculate(self) -> tuple[float]:
        map_path = Path(f"/home/akatsuki/lets/.data/beatmaps/{self.map.id}.osu")  # lol
        if not map_path.exists():
            async with ClientSession() as sesh:
                async with sesh.get(f"https://old.ppy.sh/osu/{self.map.id}") as resp:
                    map_file = await resp.read()
                    map_path.write_bytes(map_file)

        try:
            pp, sr = self.calc(self.score).calculate(str(map_path))
        except:
            pp, sr = 0, 0  # shouldn't really occur

        if math.isnan(pp) or math.isinf(pp):
            return (0.0, 0.0)

        return (pp, sr)
