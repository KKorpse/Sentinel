#!/usr/bin/env python3
"""Sentinel Cli —— Agent 回传任务结果的入口。

用法：
  # 直接给内容
  python3 sentinel_cli.py reply --task-id SENT-xxxx --content "处理完成"

  # 多行内容走标准输入（保留换行与特殊字符）
  python3 sentinel_cli.py reply --task-id SENT-xxxx --content-stdin <<'EOF'
  第一行
  第二行
  EOF

  # 附带图片
  python3 sentinel_cli.py reply --task-id SENT-xxxx --content "见图" --image out.png

同一个任务可以回传多次，每次都会单独送审，通过后才转达给最初的用户。
"""

import argparse
import base64
import json
import os
import socket
import sys
from pathlib import Path

VAR_DIR_NAME = "var"
SOCKET_FILE_NAME = "sentinel.sock"
ENV_SOCKET_PATH = "SENTINEL_SOCKET"

ACTION_REPLY = "reply"
STATUS_OK = "ok"

CONNECT_TIMEOUT_SEC = 10
RECV_CHUNK_BYTES = 64 * 1024

EXIT_OK = 0
EXIT_USAGE = 2
EXIT_FAILED = 1

BASE_DIR = Path(__file__).resolve().parent


def socket_path() -> Path:
    override = os.environ.get(ENV_SOCKET_PATH, "")
    if override:
        return Path(override)
    return BASE_DIR / VAR_DIR_NAME / SOCKET_FILE_NAME


def send(payload: dict) -> dict:
    path = socket_path()
    if not path.exists():
        raise SystemExit(f"Sentinel 服务未运行（找不到 {path}）")

    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.settimeout(CONNECT_TIMEOUT_SEC)
        sock.connect(str(path))
        sock.sendall((json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8"))
        buf = b""
        while not buf.endswith(b"\n"):
            chunk = sock.recv(RECV_CHUNK_BYTES)
            if not chunk:
                break
            buf += chunk
    if not buf:
        raise SystemExit("Sentinel 服务没有返回内容")
    return json.loads(buf.decode("utf-8"))


def read_content(args: argparse.Namespace) -> str:
    if args.content_stdin:
        return sys.stdin.read()
    if args.content_file:
        return Path(args.content_file).read_text(encoding="utf-8")
    return args.content or ""


def cmd_reply(args: argparse.Namespace) -> int:
    content = read_content(args).strip()
    if not content:
        print("内容为空，需要 --content / --content-stdin / --content-file 之一",
              file=sys.stderr)
        return EXIT_USAGE

    payload = {
        "action": ACTION_REPLY,
        "task_id": args.task_id,
        "content": content,
    }
    if args.image:
        image_path = Path(args.image)
        if not image_path.is_file():
            print(f"图片不存在: {image_path}", file=sys.stderr)
            return EXIT_USAGE
        payload["image_base64"] = base64.b64encode(image_path.read_bytes()).decode("ascii")

    resp = send(payload)
    print(json.dumps(resp, ensure_ascii=False, indent=2))
    return EXIT_OK if resp.get("status") == STATUS_OK else EXIT_FAILED


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="sentinel-cli", description="Sentinel 人工审批网关 CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    reply = sub.add_parser(ACTION_REPLY, help="回传任务结果（会先送审）")
    reply.add_argument("--task-id", required=True, help="Sentinel 任务 ID，如 SENT-xxxx")
    reply.add_argument("--content", help="回复内容")
    reply.add_argument("--content-stdin", action="store_true", help="从标准输入读回复内容")
    reply.add_argument("--content-file", help="从文件读回复内容")
    reply.add_argument("--image", help="附带的本地图片路径")
    reply.set_defaults(func=cmd_reply)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
