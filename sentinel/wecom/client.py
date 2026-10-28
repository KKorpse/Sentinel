"""企业微信机器人长连接客户端。

只管协议：连接、鉴权、心跳、请求-应答、消息推送、素材上传。
业务逻辑通过 on_callback 回调交给上层。
"""

import asyncio
import base64
import hashlib
import json
from collections.abc import Awaitable, Callable

import websockets

from ..config import logger
from . import media as media_util
from . import protocol as p
from ..ids import new_req_id

LOG = logger("wecom")

CallbackHandler = Callable[[dict], Awaitable[None]]


class BotClient:
    """一条长连接的生命周期管理。同一 BotID 只能有一条，重复连接会把旧的踢掉。"""

    def __init__(self, bot_id: str, secret: str, on_callback: CallbackHandler,
                 url: str = p.WS_URL):
        self.bot_id = bot_id
        self.secret = secret
        self.url = url
        self._on_callback = on_callback

        self._ws: websockets.WebSocketClientProtocol | None = None
        # req_id -> 等待 ack 的 Future，由单一的分发协程负责唤醒
        self._acks: dict[str, asyncio.Future] = {}
        # 后台协程引用，避免被 GC
        self._tasks: set[asyncio.Task] = set()

    # ---------- 收发原语 ----------

    async def send(self, payload: dict) -> None:
        if self._ws is None:
            raise RuntimeError("websocket 未连接")
        await self._ws.send(json.dumps(payload, ensure_ascii=False))

    async def request(self, payload: dict,
                      timeout: float = p.REQUEST_TIMEOUT_SEC) -> dict:
        """发命令并等同 req_id 的 ack。ack 由分发协程投递，避免多协程抢帧。"""
        req_id = p.frame_req_id(payload)
        fut: asyncio.Future = asyncio.get_running_loop().create_future()
        self._acks[req_id] = fut
        try:
            await self.send(payload)
            return await asyncio.wait_for(fut, timeout=timeout)
        finally:
            self._acks.pop(req_id, None)

    def spawn(self, coro) -> None:
        """起后台协程并持有引用，避免 asyncio 把它回收掉。"""
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    # ---------- 消息推送 ----------

    async def push(self, chatid: str, chat_type: int, body: dict) -> None:
        await self.send(p.frame(p.CMD_SEND_MSG, new_req_id(), {
            p.KEY_CHATID: chatid,
            "chat_type": chat_type,
            **body,
        }))

    async def push_to_user(self, userid: str, body: dict) -> None:
        """单聊主动推送：chatid 填 userid。"""
        await self.push(userid, p.CHAT_TYPE_SINGLE, body)

    async def push_to_chat(self, chatid: str, body: dict) -> None:
        """群聊主动推送：chatid 用回调里的群 chatid。"""
        await self.push(chatid, p.CHAT_TYPE_GROUP, body)

    async def respond(self, req_id: str, body: dict) -> None:
        """被动回复：req_id 必须透传回调里的那个，回复才会挂在原消息上。"""
        await self.send(p.frame(p.CMD_RESPOND_MSG, req_id, body))

    async def respond_update(self, req_id: str, body: dict) -> None:
        """更新原卡片，同样要透传回调的 req_id。"""
        await self.send(p.frame(p.CMD_RESPOND_UPDATE_MSG, req_id, body))

    # ---------- 素材 ----------

    async def upload_image(self, data: bytes,
                           filename: str = media_util.DEFAULT_IMAGE_FILENAME) -> str:
        """分片上传图片，返回 media_id（3 天有效）。"""
        total = len(data)
        chunks = [data[i: i + p.UPLOAD_CHUNK_SIZE]
                  for i in range(0, total, p.UPLOAD_CHUNK_SIZE)]

        init = await self.request(p.frame(p.CMD_UPLOAD_MEDIA_INIT, new_req_id(), {
            p.UPLOAD_KEY_TYPE: p.UPLOAD_TYPE_IMAGE,
            p.UPLOAD_KEY_FILENAME: filename,
            p.UPLOAD_KEY_TOTAL_SIZE: total,
            p.UPLOAD_KEY_TOTAL_CHUNKS: len(chunks),
            p.UPLOAD_KEY_MD5: hashlib.md5(data).hexdigest(),
        }))
        upload_id = p.frame_body(init).get(p.UPLOAD_KEY_UPLOAD_ID, "")
        if not upload_id:
            raise RuntimeError(f"上传初始化失败: {init}")

        for index, chunk in enumerate(chunks):
            resp = await self.request(
                p.frame(p.CMD_UPLOAD_MEDIA_CHUNK, new_req_id(), {
                    p.UPLOAD_KEY_UPLOAD_ID: upload_id,
                    p.UPLOAD_KEY_CHUNK_INDEX: index,
                    p.UPLOAD_KEY_BASE64_DATA: base64.b64encode(chunk).decode("ascii"),
                }))
            if resp.get(p.KEY_ERRCODE, -1) != p.ERRCODE_OK:
                raise RuntimeError(f"分片 {index} 上传失败: {resp}")

        finish = await self.request(p.frame(p.CMD_UPLOAD_MEDIA_FINISH, new_req_id(), {
            p.UPLOAD_KEY_UPLOAD_ID: upload_id,
        }))
        media_id = p.frame_body(finish).get(p.MEDIA_ID, "")
        if not media_id:
            raise RuntimeError(f"上传完成失败: {finish}")
        LOG.info("图片上传完成 media_id=%s size=%s chunks=%s",
                 media_id, total, len(chunks))
        return media_id

    # ---------- 连接生命周期 ----------

    async def _subscribe(self) -> None:
        resp = await self.request(
            p.frame(p.CMD_SUBSCRIBE, new_req_id(),
                    {"bot_id": self.bot_id, "secret": self.secret}),
            timeout=p.SUBSCRIBE_TIMEOUT_SEC)
        if resp.get(p.KEY_ERRCODE, -1) != p.ERRCODE_OK:
            raise RuntimeError(
                f"订阅失败 errcode={resp.get(p.KEY_ERRCODE)} "
                f"errmsg={resp.get(p.KEY_ERRMSG)}")
        LOG.info("订阅成功")

    async def _heartbeat(self) -> None:
        """服务端会回 ack，但这里只发不等，断连由连接本身的关闭事件感知。"""
        while True:
            await asyncio.sleep(p.HEARTBEAT_INTERVAL_SEC)
            try:
                await self.send(p.frame(p.CMD_PING, new_req_id()))
            except (websockets.ConnectionClosed, RuntimeError):
                return

    def _deliver_ack(self, data: dict) -> None:
        req_id = p.frame_req_id(data)
        fut = self._acks.get(req_id)
        if fut is not None and not fut.done():
            fut.set_result(data)
        elif data.get(p.KEY_ERRCODE, p.ERRCODE_OK) != p.ERRCODE_OK:
            # 没人等的 ack 多为 send_msg/respond_msg 的回执，只报错误
            LOG.error("命令失败 req_id=%s errcode=%s errmsg=%s", req_id,
                      data.get(p.KEY_ERRCODE), data.get(p.KEY_ERRMSG))

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
                # 单一消费者：ack 唤醒 Future，回调另起协程，
                # 否则回调里再发请求（如上传图片）会把分发堵死形成自锁
                while True:
                    data = await queue.get()
                    if data.get(p.KEY_CMD) is None:
                        self._deliver_ack(data)
                        continue
                    self.spawn(self._on_callback(data))

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
        """断线指数退避重连，直到 stop_event 被置位。"""
        backoff = p.RECONNECT_BACKOFF_INIT_SEC
        while not stop_event.is_set():
            try:
                await self._session()
                backoff = p.RECONNECT_BACKOFF_INIT_SEC
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
            backoff = min(backoff * p.RECONNECT_BACKOFF_FACTOR,
                          p.RECONNECT_BACKOFF_MAX_SEC)
