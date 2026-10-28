"""回调消息体解析。

body 和 body.quote 是同一套结构，所有解析函数都可复用。
"""

from . import protocol as p

TYPE_NAMES = {
    p.MSG_TEXT: "文本",
    p.MSG_IMAGE: "图片",
    p.MSG_VOICE: "语音",
    p.MSG_FILE: "文件",
    p.MSG_VIDEO: "视频",
    p.MSG_MIXED: "图文",
}
MEDIA_TYPES = (p.MSG_IMAGE, p.MSG_FILE, p.MSG_VIDEO)

EMPTY_VOICE_TEXT = "(语音已转文本为空)"
QUOTE_TEMPLATE = "> 引用原文（{kind}）：{content}"


def type_name(msg_type: str) -> str:
    """卡片 horizontal_content_list 的 keyname 建议 <=5 字，给中文短名。"""
    return TYPE_NAMES.get(msg_type, msg_type)


def media_of(body: dict) -> dict:
    """取 image/file/video 的资源结构体（含 url 与 aeskey）。"""
    return body.get(body.get(p.KEY_MSGTYPE, "")) or {}


def sub_items(body: dict) -> list[dict]:
    """mixed（图文混排）的子消息列表。"""
    return (body.get(p.MSG_MIXED) or {}).get(p.KEY_MSG_ITEM, [])


def extract_content(body: dict) -> str:
    """取出消息内容。

    文本给原文；媒体类型给占位说明（url 是加密资源的临时地址，放进正文没意义）。
    """
    msg_type = body.get(p.KEY_MSGTYPE, "")

    if msg_type == p.MSG_TEXT:
        return (body.get(p.MSG_TEXT) or {}).get(p.KEY_CONTENT, "")
    if msg_type == p.MSG_VOICE:
        return (body.get(p.MSG_VOICE) or {}).get(p.KEY_CONTENT, "") or EMPTY_VOICE_TEXT
    if msg_type in MEDIA_TYPES:
        name = media_of(body).get(p.MEDIA_NAME, "")
        suffix = f"：{name}" if name else ""
        return f"[{type_name(msg_type)}{suffix}]"
    if msg_type == p.MSG_MIXED:
        parts = [extract_content(item) for item in sub_items(body)]
        return " ".join(part for part in parts if part)
    return f"[{msg_type}]"


def format_quote(quote: dict | None) -> str:
    """把被引用的原消息格式化成引用块；没有引用时返回空串。"""
    if not quote:
        return ""
    content = extract_content(quote)
    if not content:
        return ""
    return QUOTE_TEMPLATE.format(
        kind=type_name(quote.get(p.KEY_MSGTYPE, "")), content=content)


def compose_request(content: str, quote_text: str) -> str:
    """请求体 = 被引用的原文（若有）+ 本次发送的内容，两者都算请求内容。"""
    return f"{quote_text}\n{content}" if quote_text else content


def iter_image_sources(body: dict):
    """按出现顺序产出所有图片的 (url, aeskey)。mixed 要递归下钻。"""
    msg_type = body.get(p.KEY_MSGTYPE, "")

    if msg_type == p.MSG_MIXED:
        for item in sub_items(body):
            yield from iter_image_sources(item)
        return

    if msg_type != p.MSG_IMAGE:
        return

    media = media_of(body)
    url = media.get(p.MEDIA_URL, "")
    if url:
        yield url, media.get(p.MEDIA_AESKEY, "")
