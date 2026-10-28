#!/usr/bin/env python3
"""企业微信智能机器人「人工审批」Demo。

流程：
  1. 任何人 @ 机器人 / 单聊发给机器人 → 机器人把原消息原封不动地以「申请卡片」推给审批人（APPROVER_USERID）
  2. 审批卡片上有「确认」「拒绝」两个按钮
  3. 审批人点「确认」→ 执行后续动作（当前实现 = echo 回原发送者）
     审批人点「拒绝」→ 通知原发送者已被拒绝，不执行

协议参考：
  - 长连接 https://developer.work.weixin.qq.com/document/path/101463
  - 模板卡片 https://developer.work.weixin.qq.com/document/path/101032
  - 接收事件 https://developer.work.weixin.qq.com/document/path/101466

用法：
  cp config.example.json config.json   # 填入 bot_id / secret / approver_userid
  python3 wecom_bot_approval_demo.py
  （config.json 已被 .gitignore 忽略，不会提交）
"""

import asyncio
import json
import logging
import os
import signal
import uuid

import websockets

WECOM_WS_URL = "wss://openws.work.weixin.qq.com"

# 配置文件（含 Secret，不纳入版本管理）
CONFIG_FILE_NAME = "config.json"
CONFIG_FILE_EXAMPLE = "config.example.json"
ENV_CONFIG_PATH = "WICKET_CONFIG"
FIELD_BOT_ID = "bot_id"
FIELD_SECRET = "secret"
FIELD_APPROVER = "approver_userid"

CMD_SUBSCRIBE = "aibot_subscribe"
CMD_PING = "ping"
CMD_MSG_CALLBACK = "aibot_msg_callback"
CMD_EVENT_CALLBACK = "aibot_event_callback"
CMD_RESPOND_MSG = "aibot_respond_msg"
CMD_RESPOND_UPDATE_MSG = "aibot_respond_update_msg"
CMD_SEND_MSG = "aibot_send_msg"

MSG_TYPE_MARKDOWN = "markdown"
MSG_TYPE_TEMPLATE_CARD = "template_card"
MSG_TYPE_TEXT = "text"

CARD_TYPE_BUTTON_INTERACTION = "button_interaction"
RESPONSE_TYPE_UPDATE_TEMPLATE_CARD = "update_template_card"

EVENT_ENTER_CHAT = "enter_chat"
EVENT_TEMPLATE_CARD_EVENT = "template_card_event"

CHATTYPE_NAME_SINGLE = "single"
CHATTYPE_NAME_GROUP = "group"

BUTTON_KEY_CONFIRM = "confirm"
BUTTON_KEY_REJECT = "reject"
BUTTON_STYLE_CONFIRM = 1
BUTTON_STYLE_REJECT = 2

# 会话类型：aibot_send_msg 主动推送时必填。单聊 chatid 填 userid，群聊填回调里的 chatid
CHAT_TYPE_SINGLE = 1
CHAT_TYPE_GROUP = 2

HEARTBEAT_INTERVAL_SEC = 30
RECONNECT_BACKOFF_MAX_SEC = 60
SUBSCRIBE_TIMEOUT_SEC = 10
CARD_UPDATE_WINDOW_SEC = 5  # template_card_event 必须在 5 秒内响应

TASK_ID_PREFIX = "approval_"
TASK_ID_MAX_LEN = 128

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
LOG = logging.getLogger("wecom-bot-approval")


def new_req_id() -> str:
    return uuid.uuid4().hex


def new_task_id() -> str:
    return TASK_ID_PREFIX + uuid.uuid4().hex[:16]


def msg_type_name(msg_type: str) -> str:
    """模板卡片里的 horizontal_content_list keyname 建议 <=5 字，这里给中文短名。"""
    mapping = {
        "text": "文本",
        "image": "图片",
        "voice": "语音",
        "file": "文件",
        "video": "视频",
        "mixed": "图文",
    }
    return mapping.get(msg_type, msg_type)


def extract_content(body: dict) -> str:
    """原封不动取出用户发来的内容。文本直接给原文，其他类型给出可读描述。"""
    msg_type = body.get("msgtype", "")
    if msg_type == "text":
        return (body.get("text") or {}).get("content", "")
    if msg_type == "voice":
        return (body.get("voice") or {}).get("content", "") or "(语音已转文本为空)"
    if msg_type == "image":
        return (body.get("image") or {}).get("url", "")
    if msg_type == "file":
        return (body.get("file") or {}).get("url", "")
    if msg_type == "video":
        return (body.get("video") or {}).get("url", "")
    if msg_type == "mixed":
        parts = []
        for item in (body.get("mixed") or {}).get("msg_item", []):
            item_type = item.get("msgtype", "")
            if item_type == "text":
                parts.append((item.get("text") or {}).get("content", ""))
            elif item_type == "image":
                parts.append((item.get("image") or {}).get("url", ""))
        return " ".join(p for p in parts if p)
    return json.dumps(body, ensure_ascii=False)


class ApprovalBot:
    def __init__(self, bot_id: str, secret: str, approver: str, url: str = WECOM_WS_URL):
        self.bot_id = bot_id
        self.secret = secret
        self.approver = approver
        self.url = url
        self._ws: websockets.WebSocketClientProtocol | None = None
        # task_id -> 待审批请求上下文
        self._pending: dict[str, dict] = {}

    # ---------- 底层收发 ----------

    async def _send(self, payload: dict) -> None:
        if self._ws is None:
            raise RuntimeError("websocket 未连接")
        await self._ws.send(json.dumps(payload, ensure_ascii=False))

    async def _request(self, payload: dict, q: asyncio.Queue,
                       timeout: float = SUBSCRIBE_TIMEOUT_SEC) -> dict:
        """发送命令并等待同 req_id 的 ack；期间收到的回调帧原样放回队列。"""
        req_id = payload["headers"]["req_id"]
        await self._send(payload)

        stashed: list[dict] = []
        try:
            async with asyncio.timeout(timeout):
                while True:
                    frame = await q.get()
                    if (frame.get("headers") or {}).get("req_id") == req_id \
                            and frame.get("cmd") is None:
                        return frame
                    stashed.append(frame)
        finally:
            for frame in stashed:
                q.put_nowait(frame)

    async def _heartbeat(self) -> None:
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL_SEC)
            try:
                await self._send({"cmd": CMD_PING, "headers": {"req_id": new_req_id()}})
            except websockets.ConnectionClosed:
                return

    async def _subscribe(self, q: asyncio.Queue) -> None:
        resp = await self._request({
            "cmd": CMD_SUBSCRIBE,
            "headers": {"req_id": new_req_id()},
            "body": {"bot_id": self.bot_id, "secret": self.secret},
        }, q)
        if resp.get("errcode", -1) != 0:
            raise RuntimeError(f"订阅失败 errcode={resp.get('errcode')} errmsg={resp.get('errmsg')}")
        LOG.info("订阅成功")

    # ---------- 业务动作 ----------

    async def _push_to_user(self, userid: str, body: dict) -> None:
        """主动推送（aibot_send_msg）：单聊时 chatid 填 userid。"""
        await self._push(chatid=userid, chat_type=CHAT_TYPE_SINGLE, body=body)

    async def _push_to_chat(self, chatid: str, body: dict) -> None:
        """主动推送到群聊：chatid 必须用回调里的群 chatid。"""
        await self._push(chatid=chatid, chat_type=CHAT_TYPE_GROUP, body=body)

    async def _push(self, chatid: str, chat_type: int, body: dict) -> None:
        await self._send({
            "cmd": CMD_SEND_MSG,
            "headers": {"req_id": new_req_id()},
            "body": {
                "chatid": chatid,
                "chat_type": chat_type,
                **body,
            },
        })

    async def _reply_to_callback(self, req_id: str, body: dict) -> None:
        """被动回复（aibot_respond_msg）：把卡片回给当前会话，req_id 必须透传。"""
        await self._send({
            "cmd": CMD_RESPOND_MSG,
            "headers": {"req_id": req_id},
            "body": body,
        })

    async def _push_text(self, userid: str, content: str) -> None:
        await self._push_to_user(userid, {
            "msgtype": MSG_TYPE_MARKDOWN,
            "markdown": {"content": content},
        })

    async def _notify_approver(self, ctx: dict) -> None:
        """把申请卡片推给审批人。task_id 用于点击回调时找回原请求。"""
        task_id = ctx["task_id"]
        card = {
            "card_type": CARD_TYPE_BUTTON_INTERACTION,
            "main_title": {
                "title": "有一条新的机器人消息待你确认",
                "desc": f"来自 {ctx['from']}（{ctx['chattype']}）",
            },
            "sub_title_text": ctx["content"],
            "horizontal_content_list": [
                {"keyname": "发起人", "value": ctx["from"]},
                {"keyname": "会话", "value": ctx["chattype"]},
                {"keyname": "群ID", "value": ctx["chatid"] or "-"},
                {"keyname": "类型", "value": msg_type_name(ctx["msgtype"])},
            ],
            "button_list": [
                {"text": "确认", "style": BUTTON_STYLE_CONFIRM, "key": BUTTON_KEY_CONFIRM},
                {"text": "拒绝", "style": BUTTON_STYLE_REJECT, "key": BUTTON_KEY_REJECT},
            ],
            "card_action": {"type": 0},
            "task_id": task_id,
        }
        await self._push_to_user(self.approver, {
            "msgtype": MSG_TYPE_TEMPLATE_CARD,
            "template_card": card,
        })
        LOG.info("已推送给审批人 %s，task_id=%s", self.approver, task_id)

    async def _execute(self, ctx: dict) -> None:
        """「确认」之后的后续步骤：目前 = echo 回原始会话（群聊回群、单聊回本人）。"""
        text = {
            "msgtype": MSG_TYPE_MARKDOWN,
            "markdown": {"content": f"你发来的消息已通过确认，echo: {ctx['content']}"},
        }
        if ctx["chattype"] == CHATTYPE_NAME_GROUP and ctx["chatid"]:
            await self._push_to_chat(ctx["chatid"], text)
            LOG.info("已 echo 到群 %s", ctx["chatid"])
        else:
            await self._push_to_user(ctx["from"], text)
            LOG.info("已 echo 给 %s", ctx["from"])

    # ---------- 回调处理 ----------

    async def _on_msg_callback(self, req_id: str, body: dict) -> None:
        sender = (body.get("from") or {}).get("userid", "")
        chattype = body.get("chattype", "")
        content = extract_content(body)

        LOG.info("收到消息 from=%s chattype=%s msgtype=%s content=%r",
                 sender, chattype, body.get("msgtype"), content)

        ctx = {
            "task_id": new_task_id()[:TASK_ID_MAX_LEN],
            "from": sender,
            "chattype": chattype,
            "chatid": body.get("chatid", ""),
            "msgid": body.get("msgid", ""),
            "msgtype": body.get("msgtype", ""),
            "content": content,
            "callback_req_id": req_id,
        }
        self._pending[ctx["task_id"]] = ctx

        # 先给发送者一个回执，再推送审批卡片
        await self._reply_to_callback(req_id, {
            "msgtype": MSG_TYPE_MARKDOWN,
            "markdown": {"content": "你的消息已提交，等待确认。"},
        })
        await self._notify_approver(ctx)

    async def _on_template_card_event(self, req_id: str, body: dict) -> None:
        event = (body.get("event") or {}).get("template_card_event") or {}
        task_id = event.get("task_id", "")
        event_key = event.get("event_key", "")
        clicker = (body.get("from") or {}).get("userid", "")

        ctx = self._pending.get(task_id)
        LOG.info("卡片点击 user=%s task_id=%s event_key=%s", clicker, task_id, event_key)

        if clicker != self.approver:
            LOG.warning("非审批人点击，忽略 user=%s", clicker)
            await self._send({
                "cmd": CMD_RESPOND_UPDATE_MSG,
                "headers": {"req_id": req_id},
                "body": {
                    "response_type": RESPONSE_TYPE_UPDATE_TEMPLATE_CARD,
                    "template_card": self._card_update("无权处理该申请", ctx, event_key),
                },
            })
            return

        if ctx is None:
            LOG.warning("task_id 无对应请求（可能已处理或服务重启）task_id=%s", task_id)
            await self._send({
                "cmd": CMD_RESPOND_UPDATE_MSG,
                "headers": {"req_id": req_id},
                "body": {
                    "response_type": RESPONSE_TYPE_UPDATE_TEMPLATE_CARD,
                    "template_card": self._card_update("该申请已失效", None, event_key),
                },
            })
            return

        # 先更新卡片（5 秒窗口内），再执行后续逻辑
        title = "已确认" if event_key == BUTTON_KEY_CONFIRM else "已拒绝"
        await self._send({
            "cmd": CMD_RESPOND_UPDATE_MSG,
            "headers": {"req_id": req_id},
            "body": {
                "response_type": RESPONSE_TYPE_UPDATE_TEMPLATE_CARD,
                "template_card": self._card_update(title, ctx, event_key),
            },
        })

        self._pending.pop(task_id, None)

        try:
            async with asyncio.timeout(CARD_UPDATE_WINDOW_SEC):
                if event_key == BUTTON_KEY_CONFIRM:
                    await self._execute(ctx)
                else:
                    await self._push_text(ctx["from"], "你发来的消息已被拒绝。")
                    LOG.info("审批拒绝，已通知 %s", ctx["from"])
        except asyncio.TimeoutError:
            LOG.error("后续处理超过 %s 秒", CARD_UPDATE_WINDOW_SEC)

    def _card_update(self, title: str, ctx: dict | None, event_key: str) -> dict:
        """更新后的卡片：去掉按钮，只显示结论。task_id 在更新场景下无效，故不传。"""
        content = ctx["content"] if ctx else "(申请不存在)"
        sender = ctx["from"] if ctx else "-"
        return {
            "card_type": CARD_TYPE_BUTTON_INTERACTION,
            "main_title": {
                "title": title,
                "desc": f"来自 {sender}",
            },
            "sub_title_text": content,
            "horizontal_content_list": [
                {"keyname": "发起人", "value": sender},
                {"keyname": "操作", "value": event_key},
            ],
            "button_list": [
                {"text": title, "style": BUTTON_STYLE_CONFIRM, "key": f"{event_key}_done"},
            ],
            "card_action": {"type": 0},
        }

    async def _on_event_callback(self, req_id: str, body: dict) -> None:
        event_type = (body.get("event") or {}).get("eventtype", "")
        LOG.info("收到事件 eventtype=%s from=%s", event_type,
                 (body.get("from") or {}).get("userid"))

        if event_type == EVENT_TEMPLATE_CARD_EVENT:
            await self._on_template_card_event(req_id, body)
        elif event_type == EVENT_ENTER_CHAT:
            await self._reply_to_callback(req_id, {
                "msgtype": MSG_TYPE_MARKDOWN,
                "markdown": {"content": "你好，发消息给我，会先转给管理员确认。"},
            })

    async def _dispatch(self, frame: dict) -> None:
        cmd = frame.get("cmd")
        if cmd is None:
            return  # 无 cmd 的帧是 ack，已在 _request 里消费
        req_id = (frame.get("headers") or {}).get("req_id", "")
        body = frame.get("body") or {}

        if cmd == CMD_MSG_CALLBACK:
            await self._on_msg_callback(req_id, body)
        elif cmd == CMD_EVENT_CALLBACK:
            await self._on_event_callback(req_id, body)
        else:
            LOG.warning("未处理的命令 cmd=%s", cmd)

    # ---------- 连接生命周期 ----------

    async def _session(self) -> None:
        q: asyncio.Queue[dict] = asyncio.Queue()

        async with websockets.connect(self.url, ping_interval=None) as ws:
            self._ws = ws
            LOG.info("websocket 已连接 %s", self.url)

            async def pump() -> None:
                async for raw in ws:
                    try:
                        await q.put(json.loads(raw))
                    except json.JSONDecodeError:
                        LOG.error("非法 JSON: %r", raw)

            reader = asyncio.ensure_future(pump())
            hb = None
            try:
                await self._subscribe(q)
                hb = asyncio.create_task(self._heartbeat())

                while True:
                    get = asyncio.ensure_future(q.get())
                    done, _ = await asyncio.wait(
                        {get, reader}, return_when=asyncio.FIRST_COMPLETED
                    )
                    if get in done:
                        frame = get.result()
                        try:
                            await self._dispatch(frame)
                        except Exception:
                            LOG.exception("处理回调失败: %r", frame)
                        continue
                    get.cancel()
                    await reader  # 抛 ConnectionClosed
                    return
            finally:
                if hb is not None:
                    hb.cancel()
                reader.cancel()
                self._ws = None
            LOG.warning("websocket 已断开")

    async def run(self, stop_event: asyncio.Event) -> None:
        backoff = 1
        while not stop_event.is_set():
            try:
                await self._session()
                backoff = 1
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
            backoff = min(backoff * 2, RECONNECT_BACKOFF_MAX_SEC)


def load_config() -> tuple[str, str, str]:
    """从 config.json 读配置（不提交到仓库），字段缺失时报错退出。"""
    path = os.environ.get(ENV_CONFIG_PATH, CONFIG_FILE_NAME)
    try:
        with open(path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except FileNotFoundError:
        raise SystemExit(f"配置文件不存在: {path}，请复制 {CONFIG_FILE_EXAMPLE} 后填入")
    except json.JSONDecodeError as e:
        raise SystemExit(f"配置文件 {path} 不是合法 JSON: {e}")

    bot_id = cfg.get(FIELD_BOT_ID, "")
    secret = cfg.get(FIELD_SECRET, "")
    approver = cfg.get(FIELD_APPROVER, "")
    missing = [
        name for name, value in (
            (FIELD_BOT_ID, bot_id),
            (FIELD_SECRET, secret),
            (FIELD_APPROVER, approver),
        ) if not value
    ]
    if missing:
        raise SystemExit(f"配置 {path} 缺少字段: " + ", ".join(missing))
    return bot_id, secret, approver


async def main() -> None:
    bot_id, secret, approver = load_config()
    bot = ApprovalBot(bot_id, secret, approver)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    await bot.run(stop_event)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
