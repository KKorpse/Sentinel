"""配置加载与运行时路径。"""

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path

CONFIG_FILE_NAME = "config.json"
CONFIG_FILE_EXAMPLE = "config.example.json"
ENV_CONFIG_PATH = "SENTINEL_CONFIG"

FIELD_BOT_ID = "bot_id"
FIELD_SECRET = "secret"
FIELD_APPROVER = "approver_userid"
FIELD_ASSIGNEE = "multica_assignee"
REQUIRED_FIELDS = (FIELD_BOT_ID, FIELD_SECRET, FIELD_APPROVER)
DEFAULT_ASSIGNEE = "zack"

VAR_DIR_NAME = "var"
ATTACHMENT_DIR_NAME = "attachments"
SOCKET_FILE_NAME = "sentinel.sock"
STATE_FILE_NAME = "state.json"
CLI_FILE_NAME = "sentinel_cli.py"

LOG_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"
LOG_NAME = "sentinel"

# 项目根目录：本文件在 <root>/sentinel/ 下
BASE_DIR = Path(__file__).resolve().parent.parent
VAR_DIR = BASE_DIR / VAR_DIR_NAME
ATTACHMENT_DIR = VAR_DIR / ATTACHMENT_DIR_NAME
SOCKET_PATH = VAR_DIR / SOCKET_FILE_NAME
STATE_PATH = VAR_DIR / STATE_FILE_NAME
CLI_PATH = BASE_DIR / CLI_FILE_NAME


@dataclass(frozen=True)
class Config:
    bot_id: str
    secret: str
    approver_userid: str
    multica_assignee: str = DEFAULT_ASSIGNEE


def config_path() -> Path:
    override = os.environ.get(ENV_CONFIG_PATH, "")
    return Path(override) if override else BASE_DIR / CONFIG_FILE_NAME


def load_config() -> Config:
    """读配置文件；缺字段直接退出，避免跑起来才报错。"""
    path = config_path()
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise SystemExit(f"配置文件不存在: {path}，请复制 {CONFIG_FILE_EXAMPLE} 后填入")
    except json.JSONDecodeError as e:
        raise SystemExit(f"配置文件 {path} 不是合法 JSON: {e}")

    missing = [name for name in REQUIRED_FIELDS if not raw.get(name)]
    if missing:
        raise SystemExit(f"配置 {path} 缺少字段: " + ", ".join(missing))

    return Config(
        bot_id=raw[FIELD_BOT_ID],
        secret=raw[FIELD_SECRET],
        approver_userid=raw[FIELD_APPROVER],
        multica_assignee=raw.get(FIELD_ASSIGNEE) or DEFAULT_ASSIGNEE,
    )


def setup_logging(level: int = logging.INFO) -> logging.Logger:
    logging.basicConfig(level=level, format=LOG_FORMAT)
    return logging.getLogger(LOG_NAME)


def logger(name: str) -> logging.Logger:
    """子模块统一挂在 sentinel.* 下，便于按模块调日志级别。"""
    return logging.getLogger(f"{LOG_NAME}.{name}")
