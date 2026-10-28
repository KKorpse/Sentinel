#!/usr/bin/env python3
"""Sentinel 服务端入口。

链路：
  1. 任何人 @ 机器人 → 原文（含引用、图片）送审给审批人，原消息上挂「等待确认」气泡
  2. 审批人点「确认」→ 在 multica 建任务并指派给 Agent
  3. Agent 执行完调用 `sentinel_cli.py reply --task-id <ID>` 回传结果（可多次）
  4. 审批人点「确认」→ 结果落回最初发消息的会话

用法：
  cp config.example.json config.json   # 填入 bot_id / secret / approver_userid
  python3 sentinel_server.py
"""

import asyncio
import signal

from sentinel.config import SOCKET_PATH, load_config, setup_logging
from sentinel.gateway import Gateway
from sentinel.ipc import server as ipc_server

STOP_SIGNALS = (signal.SIGINT, signal.SIGTERM)


def install_signal_handlers(stop_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in STOP_SIGNALS:
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass  # 非 Unix 平台不支持，退化成只能 Ctrl-C


async def main() -> None:
    setup_logging()
    gateway = Gateway(load_config())

    stop_event = asyncio.Event()
    install_signal_handlers(stop_event)

    server = await ipc_server.serve(gateway)
    try:
        await gateway.run(stop_event)
    finally:
        server.close()
        await server.wait_closed()
        SOCKET_PATH.unlink(missing_ok=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
