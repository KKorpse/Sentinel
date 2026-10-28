"""IPC 服务端：把 CLI 请求翻译成 gateway 调用。"""

import asyncio
import base64
import json
from pathlib import Path

from ..config import SOCKET_PATH, logger
from ..gateway import Gateway
from ..models import ReplyPayload
from . import protocol as proto

LOG = logger("ipc")

MSG_SUBMITTED = "回复已提交审批，通过后会转达用户"


async def _handle_reply(gateway: Gateway, payload: dict) -> dict:
    task_id = payload.get(proto.FIELD_TASK_ID, "")
    content = payload.get(proto.FIELD_CONTENT, "")
    if not task_id or not content:
        return proto.error(
            f"{proto.FIELD_TASK_ID} 和 {proto.FIELD_CONTENT} 不能为空")

    image_b64 = payload.get(proto.FIELD_IMAGE_BASE64, "")
    try:
        image = base64.b64decode(image_b64) if image_b64 else None
    except ValueError as e:
        return proto.error(f"图片 base64 解码失败: {e}")

    task = await gateway.submit_reply(
        ReplyPayload(task_id=task_id, content=content, image=image))
    if task is None:
        return proto.error(f"未知任务 {task_id}")
    return proto.ok(**{
        proto.FIELD_TASK_ID: task_id,
        proto.FIELD_MESSAGE: MSG_SUBMITTED,
    })


async def _dispatch(gateway: Gateway, payload: dict) -> dict:
    action = payload.get(proto.ACTION, "")
    if action == proto.ACTION_REPLY:
        return await _handle_reply(gateway, payload)
    return proto.error(f"未知 action: {action}")


async def serve(gateway: Gateway, socket_path: Path = SOCKET_PATH) -> asyncio.Server:
    async def on_client(reader: asyncio.StreamReader,
                        writer: asyncio.StreamWriter) -> None:
        try:
            line = await reader.readline()
            if not line:
                return
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as e:
                resp = proto.error(f"非法 JSON: {e}")
            else:
                resp = await _dispatch(gateway, payload)
            body = json.dumps(resp, ensure_ascii=False) + proto.LINE_TERMINATOR
            writer.write(body.encode("utf-8"))
            await writer.drain()
        except Exception:
            LOG.exception("处理 CLI 请求失败")
        finally:
            writer.close()

    socket_path.parent.mkdir(parents=True, exist_ok=True)
    socket_path.unlink(missing_ok=True)
    server = await asyncio.start_unix_server(
        on_client, path=str(socket_path), limit=proto.MAX_PAYLOAD_BYTES)
    LOG.info("CLI 服务已监听 %s", socket_path)
    return server
