"""消息体与模板卡片构造。"""

from . import protocol as p

CARD_TYPE_BUTTON_INTERACTION = "button_interaction"
RESPONSE_TYPE_UPDATE_TEMPLATE_CARD = "update_template_card"
CARD_ACTION_TYPE_NONE = 0

KEY_CARD_TYPE = "card_type"
KEY_MAIN_TITLE = "main_title"
KEY_TITLE = "title"
KEY_DESC = "desc"
KEY_SUB_TITLE_TEXT = "sub_title_text"
KEY_HORIZONTAL_LIST = "horizontal_content_list"
KEY_KEYNAME = "keyname"
KEY_VALUE = "value"
KEY_BUTTON_LIST = "button_list"
KEY_BUTTON_TEXT = "text"
KEY_BUTTON_STYLE = "style"
KEY_BUTTON_KEY = "key"
KEY_CARD_ACTION = "card_action"
KEY_CARD_ACTION_TYPE = "type"
KEY_RESPONSE_TYPE = "response_type"
KEY_TEMPLATE_CARD = "template_card"

BUTTON_KEY_CONFIRM = "confirm"
BUTTON_KEY_REJECT = "reject"
BUTTON_TEXT_CONFIRM = "确认"
BUTTON_TEXT_REJECT = "拒绝"
BUTTON_STYLE_BLUE = 1
BUTTON_STYLE_RED = 2
DONE_KEY_SUFFIX = "_done"

# sub_title_text 建议 <=112 字，超了服务端可能判参数非法
SUB_TITLE_MAX_LEN = 112
CLIP_SUFFIX = "…"

Rows = list[tuple[str, str]]


def clip(text: str, limit: int = SUB_TITLE_MAX_LEN) -> str:
    """卡片字段有长度上限，超长截断并压成单行。"""
    one_line = text.replace("\n", " ")
    if len(one_line) <= limit:
        return one_line
    return one_line[: limit - len(CLIP_SUFFIX)] + CLIP_SUFFIX


def markdown(content: str) -> dict:
    return {p.KEY_MSGTYPE: p.MSG_MARKDOWN, p.MSG_MARKDOWN: {p.KEY_CONTENT: content}}


def image(media_id: str) -> dict:
    return {p.KEY_MSGTYPE: p.MSG_IMAGE, p.MSG_IMAGE: {p.MEDIA_ID: media_id}}


def stream(stream_id: str, content: str, finish: bool) -> dict:
    """流式气泡：content 是全量替换，finish=True 后不可再刷。"""
    return {
        p.KEY_MSGTYPE: p.MSG_STREAM,
        p.MSG_STREAM: {
            p.STREAM_KEY_ID: stream_id,
            p.STREAM_KEY_FINISH: finish,
            p.STREAM_KEY_CONTENT: content,
        },
    }


def _rows(rows: Rows) -> list[dict]:
    return [{KEY_KEYNAME: key, KEY_VALUE: value} for key, value in rows]


def approval_card(approval_id: str, title: str, desc: str,
                  detail: str, rows: Rows) -> dict:
    """带「确认 / 拒绝」按钮的审批卡片。approval_id 放在 task_id 字段里回传。"""
    return {
        KEY_CARD_TYPE: CARD_TYPE_BUTTON_INTERACTION,
        KEY_MAIN_TITLE: {KEY_TITLE: title, KEY_DESC: desc},
        KEY_SUB_TITLE_TEXT: clip(detail),
        KEY_HORIZONTAL_LIST: _rows(rows),
        KEY_BUTTON_LIST: [
            {KEY_BUTTON_TEXT: BUTTON_TEXT_CONFIRM,
             KEY_BUTTON_STYLE: BUTTON_STYLE_BLUE, KEY_BUTTON_KEY: BUTTON_KEY_CONFIRM},
            {KEY_BUTTON_TEXT: BUTTON_TEXT_REJECT,
             KEY_BUTTON_STYLE: BUTTON_STYLE_RED, KEY_BUTTON_KEY: BUTTON_KEY_REJECT},
        ],
        KEY_CARD_ACTION: {KEY_CARD_ACTION_TYPE: CARD_ACTION_TYPE_NONE},
        p.EVENT_TASK_ID: approval_id,
    }


def closed_card(approval_id: str, title: str, detail: str,
                rows: Rows, event_key: str) -> dict:
    """结论卡片：去掉可点按钮，只留结论。

    task_id 在更新场景下不生效，但 button_interaction 的参数校验仍要求必填，
    不带会被服务端拒成 40058，所以原样回传送审时用的那个 ID。
    """
    return {
        KEY_CARD_TYPE: CARD_TYPE_BUTTON_INTERACTION,
        KEY_MAIN_TITLE: {KEY_TITLE: title},
        KEY_SUB_TITLE_TEXT: clip(detail),
        KEY_HORIZONTAL_LIST: _rows(rows),
        KEY_BUTTON_LIST: [
            {KEY_BUTTON_TEXT: title, KEY_BUTTON_STYLE: BUTTON_STYLE_BLUE,
             KEY_BUTTON_KEY: f"{event_key}{DONE_KEY_SUFFIX}"},
        ],
        KEY_CARD_ACTION: {KEY_CARD_ACTION_TYPE: CARD_ACTION_TYPE_NONE},
        p.EVENT_TASK_ID: approval_id,
    }


def template_card(card: dict) -> dict:
    return {p.KEY_MSGTYPE: p.MSG_TEMPLATE_CARD, KEY_TEMPLATE_CARD: card}


def update_template_card(card: dict) -> dict:
    return {KEY_RESPONSE_TYPE: RESPONSE_TYPE_UPDATE_TEMPLATE_CARD,
            KEY_TEMPLATE_CARD: card}
