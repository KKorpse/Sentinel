"""IPC 线上协议：unix socket 上收发单行 JSON，请求一行、响应一行。

故意不依赖 gateway / websockets，CLI 只引这个模块就够。
"""

ACTION = "action"
ACTION_REPLY = "reply"

FIELD_STATUS = "status"
FIELD_MESSAGE = "message"
FIELD_TASK_ID = "task_id"
FIELD_CONTENT = "content"
FIELD_IMAGE_BASE64 = "image_base64"

STATUS_OK = "ok"
STATUS_ERROR = "error"

MAX_PAYLOAD_BYTES = 8 * 1024 * 1024
LINE_TERMINATOR = "\n"


def ok(**fields) -> dict:
    return {FIELD_STATUS: STATUS_OK, **fields}


def error(message: str) -> dict:
    return {FIELD_STATUS: STATUS_ERROR, FIELD_MESSAGE: message}


def succeeded(resp: dict) -> bool:
    return resp.get(FIELD_STATUS) == STATUS_OK


def reply_request(task_id: str, content: str, image_base64: str = "") -> dict:
    payload = {
        ACTION: ACTION_REPLY,
        FIELD_TASK_ID: task_id,
        FIELD_CONTENT: content,
    }
    if image_base64:
        payload[FIELD_IMAGE_BASE64] = image_base64
    return payload
