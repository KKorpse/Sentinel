#!/usr/bin/env python3
"""Sentinel —— 人工审批网关服务端。

链路：
  1. 任何人 @ 机器人 / 单聊发消息 → 原文（含引用、图片）送审给审批人
  2. 审批人点「确认」→ 在 multica 建任务并指派给 Agent，任务描述里带上
     本次请求的 Sentinel 任务 ID 和回复用的 CLI 调用方式
  3. Agent 执行完调用 `sentinel_cli.py reply --task-id <ID>` 回传结果
     （同一任务可回传多次，每次都单独送审）
  4. 审批人点「确认」→ 结果原样回到最初发消息的会话

协议参考：
  - 长连接 https://developer.work.weixin.qq.com/document/path/101463
  - 模板卡片 https://developer.work.weixin.qq.com/document/path/101032

用法：
  cp config.example.json config.json   # 填入 bot_id / secret / approver_userid
  python3 sentinel_server.py
"""

import asyncio
import base64
import hashlib
import json
import logging
import os
import signal
import urllib.request
import uuid
from pathlib import Path

import websockets
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

# ---------------------------------------------------------------- 常量

WECOM_WS_URL = "wss://openws.work.weixin.qq.com"

CMD_SUBSCRIBE = "aibot_subscribe"
CMD_PING = "ping"
CMD_MSG_CALLBACK = "aibot_msg_callback"
CMD_EVENT_CALLBACK = "aibot_event_callback"
CMD_RESPOND_MSG = "aibot_respond_msg"
CMD_RESPOND_UPDATE_MSG = "aibot_respond_update_msg"
CMD_SEND_MSG = "aibot_send_msg"
CMD_UPLOAD_MEDIA_INIT = "aibot_upload_media_init"
CMD_UPLOAD_MEDIA_CHUNK = "aibot_upload_media_chunk"
CMD_UPLOAD_MEDIA_FINISH = "aibot_upload_media_finish"

MSG_TYPE_MARKDOWN = "markdown"
MSG_TYPE_TEMPLATE_CARD = "template_card"
MSG_TYPE_IMAGE = "image"

MSG_KIND_TEXT = "text"
MSG_KIND_IMAGE = "image"
MSG_KIND_VOICE = "voice"
MSG_KIND_FILE = "file"
MSG_KIND_VIDEO = "video"
MSG_KIND_MIXED = "mixed"

MEDIA_FIELD_URL = "url"
MEDIA_FIELD_AESKEY = "aeskey"
MEDIA_FIELD_NAME = "name"

UPLOAD_MEDIA_TYPE_IMAGE = "image"
UPLOAD_CHUNK_SIZE = 512 * 1024  # 单分片明文上限 512KB
DEFAULT_IMAGE_FILENAME = "image.png"

# 加密资源解密参数：AES-256-CBC，PKCS#7 补到 32 字节倍数，IV 取密钥前 16 字节
AES_KEY_LEN = 32
AES_BLOCK_SIZE = 32
AES_IV_LEN = 16
BASE64_GROUP_LEN = 4

IMAGE_MAGIC_EXT = (
    (b"\x89PNG\r\n\x1a\n", ".png"),
    (b"\xff\xd8\xff", ".jpg"),
    (b"GIF87a", ".gif"),
    (b"GIF89a", ".gif"),
)
DEFAULT_IMAGE_EXT = ".png"

CARD_TYPE_BUTTON_INTERACTION = "button_interaction"
RESPONSE_TYPE_UPDATE_TEMPLATE_CARD = "update_template_card"
CARD_ACTION_TYPE_NONE = 0

EVENT_ENTER_CHAT = "enter_chat"
EVENT_TEMPLATE_CARD_EVENT = "template_card_event"

CHATTYPE_NAME_SINGLE = "single"
CHATTYPE_NAME_GROUP = "group"

CHAT_TYPE_SINGLE = 1
CHAT_TYPE_GROUP = 2

BUTTON_KEY_CONFIRM = "confirm"
BUTTON_KEY_REJECT = "reject"
BUTTON_STYLE_CONFIRM = 1
BUTTON_STYLE_REJECT = 2

HEARTBEAT_INTERVAL_SEC = 30
RECONNECT_BACKOFF_INIT_SEC = 1
RECONNECT_BACKOFF_MAX_SEC = 60
RECONNECT_BACKOFF_FACTOR = 2
REQUEST_TIMEOUT_SEC = 30
SUBSCRIBE_TIMEOUT_SEC = 10

# 审批单类型：入站=用户请求待放行，出站=Agent 回复待放行
APPROVAL_KIND_INBOUND = "inbound"
APPROVAL_KIND_OUTBOUND = "outbound"

# 卡片 task_id 只允许数字/字母/_-@，≤128 字节
TASK_ID_PREFIX = "SENT-"
TASK_ID_RAND_LEN = 12
APPROVAL_ID_PREFIX_INBOUND = "in_"
APPROVAL_ID_PREFIX_OUTBOUND = "out_"
APPROVAL_ID_RAND_LEN = 16

# 内存里保留的请求数上限，超出按先进先出淘汰
MAX_TRACKED_REQUESTS = 500

MULTICA_BIN = "multica"
MULTICA_SUBCMD_ISSUE_CREATE = ("issue", "create")
MULTICA_SUBCMD_COMMENT_ADD = ("issue", "comment", "add")
MULTICA_OUTPUT_JSON = "json"
MULTICA_FIELD_IDENTIFIER = "identifier"
MULTICA_TIMEOUT_SEC = 120

VAR_DIR_NAME = "var"
ATTACHMENT_DIR_NAME = "attachments"
SOCKET_FILE_NAME = "sentinel.sock"

IPC_ACTION_REPLY = "reply"
IPC_STATUS_OK = "ok"
IPC_STATUS_ERROR = "error"
IPC_MAX_PAYLOAD_BYTES = 8 * 1024 * 1024

CONFIG_FILE_NAME = "config.json"
CONFIG_FILE_EXAMPLE = "config.example.json"
ENV_CONFIG_PATH = "SENTINEL_CONFIG"
FIELD_BOT_ID = "bot_id"
FIELD_SECRET = "secret"
FIELD_APPROVER = "approver_userid"
FIELD_ASSIGNEE = "multica_assignee"
DEFAULT_ASSIGNEE = "zack"

BASE_DIR = Path(__file__).resolve().parent
CLI_PATH = BASE_DIR / "sentinel_cli.py"

AGENT_PROMPT_TEMPLATE = (
    "收到该任务后，理解任务并合理设置任务的标题，"
    "然后优先搜一下当前卡有没有一模一样的任务或者是问题，有的话直接参考原对应的结果即可，"
    "再执行该任务，"
    "任务执行完成后，将结果发送给 Sentinel Cli ，带上任务 id ：{task_id}"
)
AGENT_CLI_USAGE_TEMPLATE = """\
Sentinel Cli 调用方式：

```bash
python3 {cli_path} reply --task-id {task_id} --content-stdin <<'EOF'
这里写要回复给用户的内容
EOF
```

如需回图片，追加 `--image <本地图片路径>`。

**重要**：本任务下每新增一条评论，你处理完之后都必须再调用一次上面的 Sentinel Cli
把结果发出来（任务 id 始终是 {task_id}）。不要只在首次执行后调用一次，
也不要因为是追加评论就跳过，每一轮都要发，否则提问的人收不到你的回复。"""
ISSUE_DESCRIPTION_TEMPLATE = """\
以下内容来自企业微信用户，请按原文理解：

<原始消息>
{request}
</原始消息>

{prompt}

{usage}"""
ISSUE_TITLE_TEMPLATE = "Sentinel 待处理任务 {task_id}"
COMMENT_TEMPLATE = """\
以下是提问人的追加消息，请按原文理解并继续处理：

<追加消息>
{content}
</追加消息>

处理完成后，务必再调用一次 Sentinel Cli 回传结果（任务 id：{task_id}）：

```bash
python3 {cli_path} reply --task-id {task_id} --content-stdin <<'EOF'
这里写要回复给用户的内容
EOF
```"""

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
LOG = logging.getLogger("sentinel")


# ---------------------------------------------------------------- 纯函数

def new_req_id() -> str:
    return uuid.uuid4().hex


def new_task_id() -> str:
    return TASK_ID_PREFIX + uuid.uuid4().hex[:TASK_ID_RAND_LEN]


def new_approval_id(kind: str) -> str:
    prefix = (APPROVAL_ID_PREFIX_INBOUND if kind == APPROVAL_KIND_INBOUND
              else APPROVAL_ID_PREFIX_OUTBOUND)
    return prefix + uuid.uuid4().hex[:APPROVAL_ID_RAND_LEN]


def msg_type_name(msg_type: str) -> str:
    """卡片 horizontal_content_list 的 keyname 建议 <=5 字，这里给中文短名。"""
    mapping = {
        MSG_KIND_TEXT: "文本",
        MSG_KIND_IMAGE: "图片",
        MSG_KIND_VOICE: "语音",
        MSG_KIND_FILE: "文件",
        MSG_KIND_VIDEO: "视频",
        MSG_KIND_MIXED: "图文",
    }
    return mapping.get(msg_type, msg_type)


def media_of(body: dict) -> dict:
    """取图片/文件/视频的资源结构体（含 url 与 aeskey）。"""
    return body.get(body.get("msgtype", "")) or {}


def extract_content(body: dict) -> str:
    """取出消息结构体里的内容（body 与 quote 同构）。

    文本给原文；媒体类型给占位说明（url 是加密资源的临时地址，放进正文没意义）。
    """
    msg_type = body.get("msgtype", "")
    if msg_type == MSG_KIND_TEXT:
        return (body.get(MSG_KIND_TEXT) or {}).get("content", "")
    if msg_type == MSG_KIND_VOICE:
        return (body.get(MSG_KIND_VOICE) or {}).get("content", "") or "(语音已转文本为空)"
    if msg_type in (MSG_KIND_IMAGE, MSG_KIND_FILE, MSG_KIND_VIDEO):
        name = media_of(body).get(MEDIA_FIELD_NAME, "")
        suffix = f"：{name}" if name else ""
        return f"[{msg_type_name(msg_type)}{suffix}]"
    if msg_type == MSG_KIND_MIXED:
        parts = [extract_content(item)
                 for item in (body.get(MSG_KIND_MIXED) or {}).get("msg_item", [])]
        return " ".join(p for p in parts if p)
    return f"[{msg_type}]"


def format_quote(quote: dict | None) -> str:
    """把被引用的原消息格式化成引用块；没有引用时返回空串。"""
    if not quote:
        return ""
    quoted = extract_content(quote)
    if not quoted:
        return ""
    return f"> 引用原文（{msg_type_name(quote.get('msgtype', ''))}）：{quoted}"


def compose_request(content: str, quote_text: str) -> str:
    """请求体 = 被引用的原文（若有）+ 本次发送的内容，两者都算请求内容。"""
    if quote_text:
        return f"{quote_text}\n{content}"
    return content


def derive_key(aeskey: str) -> bytes:
    """把回调里的 aeskey 归一化成 32 字节密钥。

    官方给的是 base64，现网出现过去掉 '=' 的 43 字符形态，需要自己补齐；
    同时兼容直接给 32 字节明文密钥的情况。
    """
    raw = aeskey.encode("utf-8")
    if len(raw) == AES_KEY_LEN:
        return raw
    padded = aeskey + "=" * (-len(aeskey) % BASE64_GROUP_LEN)
    key = base64.b64decode(padded)
    if len(key) != AES_KEY_LEN:
        raise ValueError(f"aeskey 解码后应为 {AES_KEY_LEN} 字节，实际 {len(key)}")
    return key


def decode_media(data: bytes, aeskey: str) -> bytes:
    """解密回调里的媒体资源：AES-256-CBC，IV 取密钥前 16 字节，PKCS#7 补到 32 字节倍数。"""
    key = derive_key(aeskey)
    decryptor = Cipher(algorithms.AES(key), modes.CBC(key[:AES_IV_LEN])).decryptor()
    plain = decryptor.update(data) + decryptor.finalize()
    pad = plain[-1]
    if not 1 <= pad <= AES_BLOCK_SIZE:
        raise ValueError(f"非法 PKCS#7 填充值 {pad}")
    return plain[:-pad]


def guess_image_ext(data: bytes) -> str:
    for magic, ext in IMAGE_MAGIC_EXT:
        if data.startswith(magic):
            return ext
    return DEFAULT_IMAGE_EXT


def render_issue_description(task_id: str, request: str) -> str:
    return ISSUE_DESCRIPTION_TEMPLATE.format(
        request=request,
        prompt=AGENT_PROMPT_TEMPLATE.format(task_id=task_id),
        usage=AGENT_CLI_USAGE_TEMPLATE.format(cli_path=CLI_PATH, task_id=task_id),
    )


def render_comment(task_id: str, content: str) -> str:
    return COMMENT_TEMPLATE.format(
        content=content, task_id=task_id, cli_path=CLI_PATH)


# ---------------------------------------------------------------- multica

async def run_multica(argv: list[str], stdin_text: str) -> dict:
    """跑一条 multica 命令，stdin 传正文，返回解析后的 JSON。"""
    proc = await asyncio.create_subprocess_exec(
        *argv,
        cwd=str(BASE_DIR),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, err = await asyncio.wait_for(
        proc.communicate(stdin_text.encode("utf-8")), timeout=MULTICA_TIMEOUT_SEC)
    if proc.returncode != 0:
        raise RuntimeError(
            f"multica 执行失败 rc={proc.returncode} argv={argv[1:4]} "
            f"err={err.decode().strip()}")
    return json.loads(out.decode("utf-8"))


async def create_issue(title: str, assignee: str, description: str,
                       attachments: list[Path]) -> str:
    """建 multica 任务并指派给 Agent，返回任务编号（如 WS-328）。"""
    argv = [
        MULTICA_BIN, *MULTICA_SUBCMD_ISSUE_CREATE,
        "--title", title,
        "--assignee", assignee,
        "--description-stdin",
        "--output", MULTICA_OUTPUT_JSON,
    ]
    for path in attachments:
        argv += ["--attachment", str(path.relative_to(BASE_DIR))]

    data = await run_multica(argv, description)
    identifier = data.get(MULTICA_FIELD_IDENTIFIER, "")
    if not identifier:
        raise RuntimeError(f"multica 返回里没有 {MULTICA_FIELD_IDENTIFIER}: {data}")
    return identifier


async def add_comment(issue_id: str, content: str, attachments: list[Path]) -> None:
    """给已有 multica 任务追加一条评论，Agent 会被唤醒继续处理。"""
    argv = [
        MULTICA_BIN, *MULTICA_SUBCMD_COMMENT_ADD, issue_id,
        "--content-stdin",
        "--output", MULTICA_OUTPUT_JSON,
    ]
    for path in attachments:
        argv += ["--attachment", str(path.relative_to(BASE_DIR))]
    await run_multica(argv, content)


# ---------------------------------------------------------------- 网关

class SentinelGateway:
    def __init__(self, bot_id: str, secret: str, approver: str, assignee: str,
                 url: str = WECOM_WS_URL):
        self.bot_id = bot_id
        self.secret = secret
        self.approver = approver
        self.assignee = assignee
        self.url = url

        self._ws: websockets.WebSocketClientProtocol | None = None
        # req_id -> 等待 ack 的 Future，由单一的分发协程负责唤醒
        self._acks: dict[str, asyncio.Future] = {}
        # 后台协程引用，避免被 GC
        self._tasks: set[asyncio.Task] = set()
        # Sentinel 任务 ID -> 原始请求上下文（可被多次回复复用，故不随审批删除）
        self._requests: dict[str, dict] = {}
        # 审批单 ID -> 审批记录
        self._approvals: dict[str, dict] = {}
        # 最近一次把 Agent 回复送审给审批人的任务 ID，审批人直接发言时续到这个任务下
        self._last_replied_task = ""

        self._attach_dir = BASE_DIR / VAR_DIR_NAME / ATTACHMENT_DIR_NAME
        self._attach_dir.mkdir(parents=True, exist_ok=True)

    # ---------- 底层收发 ----------

    async def _send(self, payload: dict) -> None:
        if self._ws is None:
            raise RuntimeError("websocket 未连接")
        await self._ws.send(json.dumps(payload, ensure_ascii=False))

    async def _request(self, payload: dict, timeout: float = REQUEST_TIMEOUT_SEC) -> dict:
        """发送命令并等待同 req_id 的 ack。ack 由分发协程投递，避免多协程抢帧。"""
        req_id = payload["headers"]["req_id"]
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self._acks[req_id] = fut
        try:
            await self._send(payload)
            return await asyncio.wait_for(fut, timeout=timeout)
        finally:
            self._acks.pop(req_id, None)

    def _spawn(self, coro) -> None:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _heartbeat(self) -> None:
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL_SEC)
            try:
                await self._send({"cmd": CMD_PING, "headers": {"req_id": new_req_id()}})
            except (websockets.ConnectionClosed, RuntimeError):
                return

    async def _subscribe(self) -> None:
        resp = await self._request({
            "cmd": CMD_SUBSCRIBE,
            "headers": {"req_id": new_req_id()},
            "body": {"bot_id": self.bot_id, "secret": self.secret},
        }, timeout=SUBSCRIBE_TIMEOUT_SEC)
        if resp.get("errcode", -1) != 0:
            raise RuntimeError(f"订阅失败 errcode={resp.get('errcode')} errmsg={resp.get('errmsg')}")
        LOG.info("订阅成功")

    # ---------- 消息发送 ----------

    async def _push(self, chatid: str, chat_type: int, body: dict) -> None:
        await self._send({
            "cmd": CMD_SEND_MSG,
            "headers": {"req_id": new_req_id()},
            "body": {"chatid": chatid, "chat_type": chat_type, **body},
        })

    async def _push_to_user(self, userid: str, body: dict) -> None:
        """单聊主动推送：chatid 填 userid。"""
        await self._push(userid, CHAT_TYPE_SINGLE, body)

    async def _push_to_chat(self, chatid: str, body: dict) -> None:
        """群聊主动推送：chatid 用回调里的群 chatid。"""
        await self._push(chatid, CHAT_TYPE_GROUP, body)

    async def _reply_to_callback(self, req_id: str, body: dict) -> None:
        """被动回复：req_id 必须透传回调里的那个。"""
        await self._send({
            "cmd": CMD_RESPOND_MSG,
            "headers": {"req_id": req_id},
            "body": body,
        })

    @staticmethod
    def _markdown(content: str) -> dict:
        return {"msgtype": MSG_TYPE_MARKDOWN, "markdown": {"content": content}}

    @staticmethod
    def _image(media_id: str) -> dict:
        return {"msgtype": MSG_TYPE_IMAGE, "image": {"media_id": media_id}}

    async def _push_to_origin(self, ctx: dict, body: dict) -> None:
        """回到最初发消息的会话：群聊回群，单聊回本人。"""
        if ctx["chattype"] == CHATTYPE_NAME_GROUP and ctx["chatid"]:
            await self._push_to_chat(ctx["chatid"], body)
            LOG.info("已送达群 %s", ctx["chatid"])
        else:
            await self._push_to_user(ctx["from"], body)
            LOG.info("已送达用户 %s", ctx["from"])

    # ---------- 媒体 ----------

    async def _fetch_media(self, url: str, aeskey: str) -> bytes:
        """下载并解密回调里的资源；url 只有 5 分钟有效期，收到就得取。"""

        def _download() -> bytes:
            with urllib.request.urlopen(url) as resp:
                return resp.read()

        raw = await asyncio.to_thread(_download)
        if not aeskey:
            return raw
        return await asyncio.to_thread(decode_media, raw, aeskey)

    async def _download_image(self, msg: dict) -> bytes | None:
        """取消息里的图片并解密。mixed（图文混排）要递归进 msg_item 找图。"""
        msg_type = msg.get("msgtype", "")

        if msg_type == MSG_KIND_MIXED:
            for item in (msg.get(MSG_KIND_MIXED) or {}).get("msg_item", []):
                image = await self._download_image(item)
                if image is not None:
                    return image
            return None

        if msg_type != MSG_KIND_IMAGE:
            return None
        media = media_of(msg)
        url = media.get(MEDIA_FIELD_URL, "")
        if not url:
            return None
        try:
            return await self._fetch_media(url, media.get(MEDIA_FIELD_AESKEY, ""))
        except Exception:
            LOG.exception("下载/解密图片失败 url=%s", url)
            return None

    async def _upload_media(self, image: bytes,
                            filename: str = DEFAULT_IMAGE_FILENAME) -> str:
        """分片上传图片素材，返回 media_id（发图片消息时要用）。"""
        total = len(image)
        chunks = [image[i: i + UPLOAD_CHUNK_SIZE]
                  for i in range(0, total, UPLOAD_CHUNK_SIZE)]

        init = await self._request({
            "cmd": CMD_UPLOAD_MEDIA_INIT,
            "headers": {"req_id": new_req_id()},
            "body": {
                "type": UPLOAD_MEDIA_TYPE_IMAGE,
                "filename": filename,
                "total_size": total,
                "total_chunks": len(chunks),
                "md5": hashlib.md5(image).hexdigest(),
            },
        })
        upload_id = init.get("body", {}).get("upload_id", "")
        if not upload_id:
            raise RuntimeError(f"上传初始化失败: {init}")

        for index, chunk in enumerate(chunks):
            resp = await self._request({
                "cmd": CMD_UPLOAD_MEDIA_CHUNK,
                "headers": {"req_id": new_req_id()},
                "body": {
                    "upload_id": upload_id,
                    "chunk_index": index,
                    "base64_data": base64.b64encode(chunk).decode("ascii"),
                },
            })
            if resp.get("errcode", -1) != 0:
                raise RuntimeError(f"分片 {index} 上传失败: {resp}")

        finish = await self._request({
            "cmd": CMD_UPLOAD_MEDIA_FINISH,
            "headers": {"req_id": new_req_id()},
            "body": {"upload_id": upload_id},
        })
        media_id = finish.get("body", {}).get("media_id", "")
        if not media_id:
            raise RuntimeError(f"上传完成失败: {finish}")
        LOG.info("图片上传完成 media_id=%s size=%s chunks=%s", media_id, total, len(chunks))
        return media_id

    def _save_attachment(self, name: str, image: bytes) -> Path:
        path = self._attach_dir / f"{name}{guess_image_ext(image)}"
        path.write_bytes(image)
        return path

    # ---------- 审批单 ----------

    def _track_request(self, ctx: dict) -> None:
        while len(self._requests) >= MAX_TRACKED_REQUESTS:
            self._requests.pop(next(iter(self._requests)))
        self._requests[ctx["task_id"]] = ctx

    def _approval_card(self, approval_id: str, title: str, desc: str,
                       detail: str, rows: list[tuple[str, str]]) -> dict:
        return {
            "card_type": CARD_TYPE_BUTTON_INTERACTION,
            "main_title": {"title": title, "desc": desc},
            "sub_title_text": detail,
            "horizontal_content_list": [
                {"keyname": key, "value": value} for key, value in rows
            ],
            "button_list": [
                {"text": "确认", "style": BUTTON_STYLE_CONFIRM, "key": BUTTON_KEY_CONFIRM},
                {"text": "拒绝", "style": BUTTON_STYLE_REJECT, "key": BUTTON_KEY_REJECT},
            ],
            "card_action": {"type": CARD_ACTION_TYPE_NONE},
            "task_id": approval_id,
        }

    @staticmethod
    def _closed_card(title: str, detail: str, rows: list[tuple[str, str]],
                     event_key: str) -> dict:
        """结论卡片：更新场景下 task_id 无效，故不带。"""
        return {
            "card_type": CARD_TYPE_BUTTON_INTERACTION,
            "main_title": {"title": title, "desc": ""},
            "sub_title_text": detail,
            "horizontal_content_list": [
                {"keyname": key, "value": value} for key, value in rows
            ],
            "button_list": [
                {"text": title, "style": BUTTON_STYLE_CONFIRM, "key": f"{event_key}_done"},
            ],
            "card_action": {"type": CARD_ACTION_TYPE_NONE},
        }

    async def _ask_inbound(self, ctx: dict) -> None:
        """把用户请求送审：先发原文（必要时连图），再发带按钮的卡片。"""
        approval_id = new_approval_id(APPROVAL_KIND_INBOUND)
        self._approvals[approval_id] = {
            "kind": APPROVAL_KIND_INBOUND,
            "task_id": ctx["task_id"],
        }

        await self._push_to_user(self.approver, self._markdown(
            f"**待确认请求** `{ctx['task_id']}`\n"
            f"来自 {ctx['from']}（{ctx['chattype']}）\n\n{ctx['request']}"))

        if ctx["image"] and not ctx["media_id"]:
            try:
                ctx["media_id"] = await self._upload_media(ctx["image"])
            except Exception:
                LOG.exception("上传请求图片失败 task_id=%s", ctx["task_id"])
        if ctx["media_id"]:
            await self._push_to_user(self.approver, self._image(ctx["media_id"]))

        await self._push_to_user(self.approver, {
            "msgtype": MSG_TYPE_TEMPLATE_CARD,
            "template_card": self._approval_card(
                approval_id,
                "有一条新的请求待你确认",
                f"来自 {ctx['from']}（{ctx['chattype']}）",
                ctx["request"],
                [
                    ("任务", ctx["task_id"]),
                    ("发起人", ctx["from"]),
                    ("会话", ctx["chattype"]),
                    ("类型", msg_type_name(ctx["msgtype"])),
                    ("图片", "有" if ctx["image"] else "无"),
                ],
            ),
        })
        LOG.info("请求已送审 task_id=%s approval=%s", ctx["task_id"], approval_id)

    async def _ask_outbound(self, ctx: dict, content: str, image: bytes | None) -> str:
        """把 Agent 回复送审。同一任务可多次调用，每次生成独立审批单。"""
        approval_id = new_approval_id(APPROVAL_KIND_OUTBOUND)
        record = {
            "kind": APPROVAL_KIND_OUTBOUND,
            "task_id": ctx["task_id"],
            "content": content,
            "media_id": "",
        }
        self._approvals[approval_id] = record

        await self._push_to_user(self.approver, self._markdown(
            f"**待确认回复** `{ctx['task_id']}`\n"
            f"将回给 {ctx['from']}（{ctx['chattype']}）\n\n{content}"))

        if image:
            try:
                record["media_id"] = await self._upload_media(image)
                await self._push_to_user(self.approver, self._image(record["media_id"]))
            except Exception:
                LOG.exception("上传回复图片失败 task_id=%s", ctx["task_id"])

        await self._push_to_user(self.approver, {
            "msgtype": MSG_TYPE_TEMPLATE_CARD,
            "template_card": self._approval_card(
                approval_id,
                "Agent 回复待你确认",
                f"任务 {ctx['task_id']}",
                content,
                [
                    ("任务", ctx["task_id"]),
                    ("单号", ctx["issue_id"] or "-"),
                    ("回给", ctx["from"]),
                    ("会话", ctx["chattype"]),
                    ("图片", "有" if image else "无"),
                ],
            ),
        })
        LOG.info("回复已送审 task_id=%s approval=%s", ctx["task_id"], approval_id)
        self._last_replied_task = ctx["task_id"]
        return approval_id

    # ---------- 审批放行后的动作 ----------

    async def _dispatch_to_agent(self, ctx: dict) -> None:
        """入站放行：建 multica 任务并指派给 Agent。"""
        attachments = []
        if ctx["image"]:
            attachments.append(self._save_attachment(ctx["task_id"], ctx["image"]))

        try:
            issue_id = await create_issue(
                title=ISSUE_TITLE_TEMPLATE.format(task_id=ctx["task_id"]),
                assignee=self.assignee,
                description=render_issue_description(ctx["task_id"], ctx["request"]),
                attachments=attachments,
            )
        except Exception as e:
            LOG.exception("建单失败 task_id=%s", ctx["task_id"])
            await self._push_to_user(self.approver, self._markdown(
                f"任务 `{ctx['task_id']}` 建单失败：{e}"))
            return

        ctx["issue_id"] = issue_id
        LOG.info("已建单 task_id=%s issue=%s assignee=%s",
                 ctx["task_id"], issue_id, self.assignee)
        await self._push_to_user(self.approver, self._markdown(
            f"任务 `{ctx['task_id']}` 已派给 **{self.assignee}**，单号 `{issue_id}`。"))

    async def _deliver_reply(self, ctx: dict, record: dict) -> None:
        """出站放行：把 Agent 回复原样送回最初的会话。"""
        await self._push_to_origin(ctx, self._markdown(record["content"]))
        if record["media_id"]:
            await self._push_to_origin(ctx, self._image(record["media_id"]))

    # ---------- 回调处理 ----------

    async def _forward_as_comment(self, req_id: str, request: str,
                                  image: bytes | None) -> None:
        """审批人单聊直接发言：作为评论续到最近一次回过话的任务下，Agent 会继续处理。"""
        ctx = self._requests.get(self._last_replied_task)
        if ctx is None or not ctx["issue_id"]:
            await self._reply_to_callback(req_id, self._markdown(
                "当前没有可追问的任务，请等 Agent 回复过一次之后再发言。"))
            return

        attachments = []
        if image:
            attachments.append(self._save_attachment(
                f"{ctx['task_id']}_{new_approval_id(APPROVAL_KIND_OUTBOUND)}", image))

        try:
            await add_comment(
                issue_id=ctx["issue_id"],
                content=render_comment(ctx["task_id"], request),
                attachments=attachments,
            )
        except Exception as e:
            LOG.exception("追加评论失败 task_id=%s issue=%s",
                          ctx["task_id"], ctx["issue_id"])
            await self._reply_to_callback(req_id, self._markdown(f"追加评论失败：{e}"))
            return

        LOG.info("已追加评论 task_id=%s issue=%s", ctx["task_id"], ctx["issue_id"])
        await self._reply_to_callback(req_id, self._markdown(
            f"已作为评论追加到任务 `{ctx['task_id']}`（单号 `{ctx['issue_id']}`），"
            f"Agent 处理完会再送审。"))

    async def _on_msg_callback(self, req_id: str, body: dict) -> None:
        sender = (body.get("from") or {}).get("userid", "")
        chattype = body.get("chattype", "")
        msg_type = body.get("msgtype", "")
        content = extract_content(body)
        quote_text = format_quote(body.get("quote"))

        LOG.info("收到消息 from=%s chattype=%s msgtype=%s content=%r quote=%r",
                 sender, chattype, msg_type, content, quote_text)

        image = await self._download_image(body)
        if image is None and body.get("quote"):
            image = await self._download_image(body["quote"])

        # 审批人单聊发言视为对 Agent 追问；群里 @ 仍按新请求走审批
        if sender == self.approver and chattype == CHATTYPE_NAME_SINGLE:
            await self._forward_as_comment(
                req_id, compose_request(content, quote_text), image)
            return

        ctx = {
            "task_id": new_task_id(),
            "from": sender,
            "chattype": chattype,
            "chatid": body.get("chatid", ""),
            "msgid": body.get("msgid", ""),
            "msgtype": msg_type,
            "content": content,
            "quote": quote_text,
            "request": compose_request(content, quote_text),
            "image": image,
            "media_id": "",
            "issue_id": "",
        }
        self._track_request(ctx)

        await self._reply_to_callback(req_id, self._markdown(
            f"已收到，等待确认（任务 `{ctx['task_id']}`）。"))
        await self._ask_inbound(ctx)

    async def _on_template_card_event(self, req_id: str, body: dict) -> None:
        event = (body.get("event") or {}).get(EVENT_TEMPLATE_CARD_EVENT) or {}
        approval_id = event.get("task_id", "")
        event_key = event.get("event_key", "")
        clicker = (body.get("from") or {}).get("userid", "")

        LOG.info("卡片点击 user=%s approval=%s event_key=%s",
                 clicker, approval_id, event_key)

        async def close_card(title: str, rows: list[tuple[str, str]], detail: str) -> None:
            await self._send({
                "cmd": CMD_RESPOND_UPDATE_MSG,
                "headers": {"req_id": req_id},
                "body": {
                    "response_type": RESPONSE_TYPE_UPDATE_TEMPLATE_CARD,
                    "template_card": self._closed_card(title, detail, rows, event_key),
                },
            })

        if clicker != self.approver:
            LOG.warning("非审批人点击，忽略 user=%s", clicker)
            await close_card("无权处理该审批", [("操作人", clicker)], "")
            return

        record = self._approvals.pop(approval_id, None)
        if record is None:
            LOG.warning("审批单不存在或已处理 approval=%s", approval_id)
            await close_card("该审批已失效", [("单据", approval_id)], "")
            return

        ctx = self._requests.get(record["task_id"])
        if ctx is None:
            LOG.warning("任务上下文已淘汰 task_id=%s", record["task_id"])
            await close_card("原始请求已过期", [("任务", record["task_id"])], "")
            return

        approved = event_key == BUTTON_KEY_CONFIRM
        title = "已确认" if approved else "已拒绝"
        detail = record.get("content", ctx["request"])
        await close_card(title, [("任务", ctx["task_id"]), ("操作", event_key)], detail)

        if record["kind"] == APPROVAL_KIND_INBOUND:
            if approved:
                self._spawn(self._dispatch_to_agent(ctx))
            else:
                self._spawn(self._push_to_origin(
                    ctx, self._markdown("你的请求已被拒绝。")))
            return

        if approved:
            self._spawn(self._deliver_reply(ctx, record))
        else:
            LOG.info("回复被拒绝，不下发 task_id=%s", ctx["task_id"])

    async def _on_event_callback(self, req_id: str, body: dict) -> None:
        event_type = (body.get("event") or {}).get("eventtype", "")
        LOG.info("收到事件 eventtype=%s from=%s", event_type,
                 (body.get("from") or {}).get("userid"))

        if event_type == EVENT_TEMPLATE_CARD_EVENT:
            await self._on_template_card_event(req_id, body)
        elif event_type == EVENT_ENTER_CHAT:
            await self._reply_to_callback(req_id, self._markdown(
                "你好，发消息给我，会先转给管理员确认后再处理。"))

    async def _handle(self, frame: dict) -> None:
        cmd = frame.get("cmd")
        req_id = (frame.get("headers") or {}).get("req_id", "")
        body = frame.get("body") or {}
        try:
            if cmd == CMD_MSG_CALLBACK:
                await self._on_msg_callback(req_id, body)
            elif cmd == CMD_EVENT_CALLBACK:
                await self._on_event_callback(req_id, body)
            else:
                LOG.warning("未处理的命令 cmd=%s", cmd)
        except Exception:
            LOG.exception("处理回调失败: %r", frame)

    # ---------- CLI 入口（本机 unix socket） ----------

    async def handle_cli_reply(self, payload: dict) -> dict:
        """Agent 通过 sentinel_cli.py 回传结果，落到出站审批。"""
        task_id = payload.get("task_id", "")
        content = payload.get("content", "")
        image_b64 = payload.get("image_base64", "")

        if not task_id or not content:
            return {"status": IPC_STATUS_ERROR, "message": "task_id 和 content 不能为空"}

        ctx = self._requests.get(task_id)
        if ctx is None:
            return {"status": IPC_STATUS_ERROR, "message": f"未知任务 {task_id}"}

        image = base64.b64decode(image_b64) if image_b64 else None
        approval_id = await self._ask_outbound(ctx, content, image)
        return {
            "status": IPC_STATUS_OK,
            "task_id": task_id,
            "approval_id": approval_id,
            "message": "回复已提交审批，通过后会转达用户",
        }

    # ---------- 连接生命周期 ----------

    async def _session(self) -> None:
        queue: asyncio.Queue[dict] = asyncio.Queue()

        async with websockets.connect(self.url, ping_interval=None) as ws:
            self._ws = ws
            LOG.info("websocket 已连接 %s", self.url)

            async def pump() -> None:
                async for raw in ws:
                    try:
                        await queue.put(json.loads(raw))
                    except json.JSONDecodeError:
                        LOG.error("非法 JSON: %r", raw)

            async def dispatch() -> None:
                # 单一消费者：ack 唤醒 Future，回调另起协程，避免回调里再发请求时自锁
                while True:
                    frame = await queue.get()
                    if frame.get("cmd") is None:
                        req_id = (frame.get("headers") or {}).get("req_id", "")
                        fut = self._acks.get(req_id)
                        if fut is not None and not fut.done():
                            fut.set_result(frame)
                        elif frame.get("errcode", 0) != 0:
                            # 没人等的 ack 多为 send_msg/respond_msg 的回执，只报错误
                            LOG.error("命令失败 req_id=%s errcode=%s errmsg=%s", req_id,
                                      frame.get("errcode"), frame.get("errmsg"))
                        continue
                    self._spawn(self._handle(frame))

            reader = asyncio.ensure_future(pump())
            dispatcher = asyncio.ensure_future(dispatch())
            heartbeat = None
            try:
                await self._subscribe()
                heartbeat = asyncio.ensure_future(self._heartbeat())
                await reader  # 断线时抛 ConnectionClosed
            finally:
                for task in (dispatcher, heartbeat):
                    if task is not None:
                        task.cancel()
                reader.cancel()
                self._ws = None
                for fut in self._acks.values():
                    if not fut.done():
                        fut.cancel()
                self._acks.clear()
            LOG.warning("websocket 已断开")

    async def run(self, stop_event: asyncio.Event) -> None:
        backoff = RECONNECT_BACKOFF_INIT_SEC
        while not stop_event.is_set():
            try:
                await self._session()
                backoff = RECONNECT_BACKOFF_INIT_SEC
            except websockets.ConnectionClosed as e:
                LOG.warning("连接关闭 code=%s reason=%s", e.code, e.reason)
            except Exception:
                LOG.exception("会话异常")
            if stop_event.is_set():
                break
            LOG.info("%s 秒后重连", backoff)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=backoff)
                break
            except asyncio.TimeoutError:
                pass
            backoff = min(backoff * RECONNECT_BACKOFF_FACTOR, RECONNECT_BACKOFF_MAX_SEC)


# ---------------------------------------------------------------- CLI 服务

async def serve_cli(gateway: SentinelGateway, socket_path: Path) -> asyncio.Server:
    """本机 unix socket，收 sentinel_cli.py 的单行 JSON 请求，回单行 JSON。"""

    async def on_client(reader: asyncio.StreamReader,
                        writer: asyncio.StreamWriter) -> None:
        try:
            line = await reader.readline()
            if not line:
                return
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as e:
                resp = {"status": IPC_STATUS_ERROR, "message": f"非法 JSON: {e}"}
            else:
                action = payload.get("action", "")
                if action == IPC_ACTION_REPLY:
                    resp = await gateway.handle_cli_reply(payload)
                else:
                    resp = {"status": IPC_STATUS_ERROR, "message": f"未知 action: {action}"}
            writer.write((json.dumps(resp, ensure_ascii=False) + "\n").encode("utf-8"))
            await writer.drain()
        except Exception:
            LOG.exception("处理 CLI 请求失败")
        finally:
            writer.close()

    socket_path.parent.mkdir(parents=True, exist_ok=True)
    if socket_path.exists():
        socket_path.unlink()
    server = await asyncio.start_unix_server(
        on_client, path=str(socket_path), limit=IPC_MAX_PAYLOAD_BYTES)
    LOG.info("CLI 服务已监听 %s", socket_path)
    return server


# ---------------------------------------------------------------- 入口

def load_config() -> dict:
    path = os.environ.get(ENV_CONFIG_PATH, str(BASE_DIR / CONFIG_FILE_NAME))
    try:
        with open(path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except FileNotFoundError:
        raise SystemExit(f"配置文件不存在: {path}，请复制 {CONFIG_FILE_EXAMPLE} 后填入")
    except json.JSONDecodeError as e:
        raise SystemExit(f"配置文件 {path} 不是合法 JSON: {e}")

    missing = [name for name in (FIELD_BOT_ID, FIELD_SECRET, FIELD_APPROVER)
               if not cfg.get(name)]
    if missing:
        raise SystemExit(f"配置 {path} 缺少字段: " + ", ".join(missing))
    cfg.setdefault(FIELD_ASSIGNEE, DEFAULT_ASSIGNEE)
    return cfg


async def main() -> None:
    cfg = load_config()
    gateway = SentinelGateway(
        bot_id=cfg[FIELD_BOT_ID],
        secret=cfg[FIELD_SECRET],
        approver=cfg[FIELD_APPROVER],
        assignee=cfg[FIELD_ASSIGNEE],
    )

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    socket_path = BASE_DIR / VAR_DIR_NAME / SOCKET_FILE_NAME
    server = await serve_cli(gateway, socket_path)
    try:
        await gateway.run(stop_event)
    finally:
        server.close()
        await server.wait_closed()
        if socket_path.exists():
            socket_path.unlink()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
