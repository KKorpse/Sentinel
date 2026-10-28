"""企业微信智能机器人长连接协议常量。

文档：
  长连接   https://developer.work.weixin.qq.com/document/path/101463
  模板卡片 https://developer.work.weixin.qq.com/document/path/101032
"""

WS_URL = "wss://openws.work.weixin.qq.com"

# ---- 命令字 ----
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

# ---- 帧字段 ----
KEY_CMD = "cmd"
KEY_HEADERS = "headers"
KEY_REQ_ID = "req_id"
KEY_BODY = "body"
KEY_ERRCODE = "errcode"
KEY_ERRMSG = "errmsg"
ERRCODE_OK = 0

# ---- 消息类型 ----
MSG_TEXT = "text"
MSG_IMAGE = "image"
MSG_VOICE = "voice"
MSG_FILE = "file"
MSG_VIDEO = "video"
MSG_MIXED = "mixed"
MSG_MARKDOWN = "markdown"
MSG_TEMPLATE_CARD = "template_card"
MSG_STREAM = "stream"

KEY_MSGTYPE = "msgtype"
KEY_CONTENT = "content"
KEY_MSG_ITEM = "msg_item"
KEY_QUOTE = "quote"
KEY_FROM = "from"
KEY_USERID = "userid"
KEY_CHATID = "chatid"
KEY_CHATTYPE = "chattype"
KEY_MSGID = "msgid"

# ---- 媒体资源 ----
MEDIA_URL = "url"
MEDIA_AESKEY = "aeskey"
MEDIA_NAME = "name"
MEDIA_ID = "media_id"

UPLOAD_TYPE_IMAGE = "image"
UPLOAD_CHUNK_SIZE = 512 * 1024  # 单分片明文上限 512KB
UPLOAD_KEY_TYPE = "type"
UPLOAD_KEY_FILENAME = "filename"
UPLOAD_KEY_TOTAL_SIZE = "total_size"
UPLOAD_KEY_TOTAL_CHUNKS = "total_chunks"
UPLOAD_KEY_MD5 = "md5"
UPLOAD_KEY_UPLOAD_ID = "upload_id"
UPLOAD_KEY_CHUNK_INDEX = "chunk_index"
UPLOAD_KEY_BASE64_DATA = "base64_data"

# ---- 会话 ----
CHATTYPE_SINGLE = "single"
CHATTYPE_GROUP = "group"
CHAT_TYPE_SINGLE = 1
CHAT_TYPE_GROUP = 2

# ---- 事件 ----
KEY_EVENT = "event"
KEY_EVENTTYPE = "eventtype"
EVENT_ENTER_CHAT = "enter_chat"
EVENT_TEMPLATE_CARD = "template_card_event"
EVENT_KEY = "event_key"
EVENT_TASK_ID = "task_id"

# ---- 流式气泡 ----
STREAM_KEY_ID = "id"
STREAM_KEY_FINISH = "finish"
STREAM_KEY_CONTENT = "content"
# 官方窗口 10 分钟，留一分钟余量
STREAM_WINDOW_SEC = 9 * 60

# ---- 连接行为 ----
HEARTBEAT_INTERVAL_SEC = 30
REQUEST_TIMEOUT_SEC = 30
SUBSCRIBE_TIMEOUT_SEC = 10
RECONNECT_BACKOFF_INIT_SEC = 1
RECONNECT_BACKOFF_MAX_SEC = 60
RECONNECT_BACKOFF_FACTOR = 2


def frame(cmd: str, req_id: str, body: dict | None = None) -> dict:
    """组一条上行帧。"""
    payload = {KEY_CMD: cmd, KEY_HEADERS: {KEY_REQ_ID: req_id}}
    if body is not None:
        payload[KEY_BODY] = body
    return payload


def frame_req_id(data: dict) -> str:
    return (data.get(KEY_HEADERS) or {}).get(KEY_REQ_ID, "")


def frame_body(data: dict) -> dict:
    return data.get(KEY_BODY) or {}


def sender_of(body: dict) -> str:
    return (body.get(KEY_FROM) or {}).get(KEY_USERID, "")


def event_of(body: dict) -> dict:
    return body.get(KEY_EVENT) or {}
