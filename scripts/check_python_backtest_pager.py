#!/usr/bin/env python3
"""Offline oracle: execute the installed official _gen_serial, without login.

Only channels/entities are substituted. The generator body is compiled verbatim
from the selected source; requests are captured, never sent to a network.
"""
import argparse
import ast
import asyncio
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace


class Entity(dict):
    def __init__(self):
        super().__init__(_listener=set())


def get_obj(root, path):
    for key in path:
        root = root.setdefault(key, Entity())
    return root


class Channel:
    def __init__(self, api, **_):
        self.queue = asyncio.Queue()
        api.channel = self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.queue.get()


async def run(generator, duration):
    api = SimpleNamespace(channel=None)
    data = Entity()
    trace = []
    self = SimpleNamespace(_api=api, _data=data, _current_dt=1000, _end_dt=2000,
                           _serials={("SHFE.test", duration): {"chart_id_set": {"user"}}},
                           _get_quotes_from_tick=lambda *_: {},
                           _get_quotes_from_kline_open=lambda *_: {},
                           _get_quotes_from_kline=lambda *_: {})
    get_obj(data, ["quotes", "SHFE.test"])

    class Sender:
        async def send(self, command):
            trace.append(command.copy())
            if not command["ins_list"]:
                return
            # Page zero precedes start, page one contains exact start, page two
            # exceeds end. A fourth request must be sent before termination.
            page = sum(bool(item["ins_list"]) for item in trace) - 1
            bounds = [(0, 1), (1, 3), (3, 4), (4, 4)][min(page, 3)]
            chart = get_obj(data, ["charts", command["chart_id"]])
            chart.update(state=command.copy(), left_id=bounds[0], right_id=bounds[1], more_data=False)
            path = ["ticks", "SHFE.test"] if duration == 0 else ["klines", "SHFE.test", str(duration)]
            serial = get_obj(data, path)
            serial.update(last_id=4)
            rows = serial.setdefault("data", {})
            for ident in range(bounds[0], bounds[1] + 1):
                rows[str(ident)] = dict(datetime=[900, 999, 1000, 1100, 2100][ident],
                    open=1.0, high=2.0, low=0.5, close=1.5, volume=1, open_oi=2, close_oi=3)
            data["mdhis_more_data"] = False
            await api.channel.queue.put(True)

    self._md_send_chan = Sender()
    serial = generator(self, "SHFE.test", duration)
    try:
        async for _ in serial:
            pass
    finally:
        await serial.aclose()
    ids = {}
    for command in trace:
        ids.setdefault(command["chart_id"], chr(ord("A") + len(ids)))
        command["chart_id"] = ids[command["chart_id"]]
    assert len(ids) == 2, trace
    active = [command for command in trace if command["ins_list"]]
    # Long duration stops at the first close; use Tick for the full A/B/A trace.
    assert active[0]["view_width"] == active[0]["focus_position"] == 8964
    if duration == 0:
        assert [command["chart_id"] for command in active] == ["A", "B", "A", "B"]
        assert [command.get("left_kline_id") for command in active] == [None, 1, 3, 4]
    assert [command["ins_list"] for command in trace[-2:]] == ["", ""]
    return trace


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=Path("/opt/tqsdk-python/tqsdk/backtest.py"))
    args = parser.parse_args()
    source = args.source.read_bytes()
    tree = ast.parse(source)
    cls = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "TqBacktest")
    function = next(node for node in cls.body if isinstance(node, ast.AsyncFunctionDef) and node.name == "_gen_serial")
    counter = iter(range(100))
    namespace = {"TqChan": Channel, "_get_obj": get_obj,
                 "_generate_uuid": lambda *_: f"chart-{next(counter)}",
                 "_get_trading_day_start_time": lambda value: value - 21_600_000_000_000}
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(args.source), "exec"), namespace)
    traces = {str(duration): asyncio.run(asyncio.wait_for(run(namespace["_gen_serial"], duration), 5))
              for duration in [0, 60_000_000_000]}
    print(json.dumps({"source_sha256": hashlib.sha256(source).hexdigest(), "traces": traces}, indent=2))


if __name__ == "__main__":
    main()
