"""multica CLI 封装。"""

import asyncio
import json
from pathlib import Path

from .config import BASE_DIR, logger

LOG = logger("multica")

BIN = "multica"
SUBCMD_ISSUE_CREATE = ("issue", "create")
SUBCMD_ISSUE_GET = ("issue", "get")
SUBCMD_ISSUE_UPDATE = ("issue", "update")
SUBCMD_COMMENT_ADD = ("issue", "comment", "add")

OPT_TITLE = "--title"
OPT_ASSIGNEE = "--assignee"
OPT_DESCRIPTION_STDIN = "--description-stdin"
OPT_CONTENT_STDIN = "--content-stdin"
OPT_ATTACHMENT = "--attachment"
OPT_OUTPUT = "--output"
OUTPUT_JSON = "json"

FIELD_IDENTIFIER = "identifier"
FIELD_TITLE = "title"

TIMEOUT_SEC = 120


async def run(argv: list[str], stdin_text: str = "") -> dict:
    """跑一条 multica 命令，stdin 传正文，返回解析后的 JSON。"""
    proc = await asyncio.create_subprocess_exec(
        *argv,
        cwd=str(BASE_DIR),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, err = await asyncio.wait_for(
        proc.communicate(stdin_text.encode("utf-8")), timeout=TIMEOUT_SEC)
    if proc.returncode != 0:
        raise RuntimeError(
            f"multica 执行失败 rc={proc.returncode} argv={argv[1:4]} "
            f"err={err.decode().strip()}")
    return json.loads(out.decode("utf-8"))


def _attachment_args(attachments: list[Path]) -> list[str]:
    """CLI 在 BASE_DIR 下执行，附件传相对路径。"""
    args: list[str] = []
    for path in attachments:
        args += [OPT_ATTACHMENT, str(path.relative_to(BASE_DIR))]
    return args


async def create_issue(title: str, assignee: str, description: str,
                       attachments: list[Path] | None = None) -> str:
    """建任务并指派给 Agent，返回任务编号（如 WS-328）。"""
    argv = [
        BIN, *SUBCMD_ISSUE_CREATE,
        OPT_TITLE, title,
        OPT_ASSIGNEE, assignee,
        OPT_DESCRIPTION_STDIN,
        OPT_OUTPUT, OUTPUT_JSON,
        *_attachment_args(attachments or []),
    ]
    data = await run(argv, description)
    identifier = data.get(FIELD_IDENTIFIER, "")
    if not identifier:
        raise RuntimeError(f"multica 返回里没有 {FIELD_IDENTIFIER}: {data}")
    return identifier


async def add_comment(issue_id: str, content: str,
                      attachments: list[Path] | None = None) -> None:
    """追加评论，Agent 会被唤醒继续处理。"""
    argv = [
        BIN, *SUBCMD_COMMENT_ADD, issue_id,
        OPT_CONTENT_STDIN,
        OPT_OUTPUT, OUTPUT_JSON,
        *_attachment_args(attachments or []),
    ]
    await run(argv, content)


async def get_title(issue_id: str) -> str:
    """取任务当前标题。Agent 会按内容改标题，所以每次都现查，不用建单时的占位名。"""
    data = await run([BIN, *SUBCMD_ISSUE_GET, issue_id, OPT_OUTPUT, OUTPUT_JSON])
    return data.get(FIELD_TITLE, "")
