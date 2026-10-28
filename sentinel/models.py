"""领域模型。

字段名与 var/state.json 里的键一一对应，便于直接 JSON 往返。
"""

import time
from dataclasses import asdict, dataclass, field, fields

from .wecom import protocol as p

# 审批单类型：入站 = 用户请求待放行，出站 = Agent 回复待放行
KIND_INBOUND = "inbound"
KIND_OUTBOUND = "outbound"

# 旧状态文件里的键名 -> 现字段名。`from` 是关键字，没法直接当字段名
LEGACY_KEYS = {"from": "sender"}


def _only_known(cls, raw: dict) -> dict:
    """做键名迁移，并丢掉已废弃的字段，避免老状态文件构造报错。"""
    names = {f.name for f in fields(cls)}
    migrated = {LEGACY_KEYS.get(k, k): v for k, v in raw.items()}
    return {k: v for k, v in migrated.items() if k in names}


@dataclass
class TaskContext:
    """一次用户请求的完整上下文，可被多次回复复用，故不随审批删除。"""

    task_id: str
    # 来源会话
    sender: str = ""
    chattype: str = ""
    chatid: str = ""
    msgid: str = ""
    msgtype: str = ""
    # 请求内容：request = quote + content
    content: str = ""
    quote: str = ""
    request: str = ""
    # 图片存盘不存内存，状态才能序列化、重启后也能重新上传
    image_path: str = ""
    media_id: str = ""
    # multica 单号
    issue_id: str = ""
    # 流式气泡：挂在用户这条消息上，审批前后一路刷新到最终答复
    callback_req_id: str = ""
    stream_id: str = ""
    stream_done: bool = False
    stream_deadline: float = 0.0
    # 已下发给用户的答复条数，首条走气泡，之后带标题另发
    reply_seq: int = 0

    @property
    def is_group(self) -> bool:
        return self.chattype == p.CHATTYPE_GROUP and bool(self.chatid)

    @property
    def dispatched(self) -> bool:
        """是否已建单。没建单就没法追加评论。"""
        return bool(self.issue_id)

    @property
    def stream_alive(self) -> bool:
        """气泡是否还能刷新：没收尾，且没超过官方刷新窗口。"""
        if self.stream_done or not self.stream_id:
            return False
        return time.monotonic() < self.stream_deadline

    def open_stream(self, stream_id: str, callback_req_id: str) -> None:
        self.stream_id = stream_id
        self.callback_req_id = callback_req_id
        self.stream_done = False
        self.stream_deadline = time.monotonic() + p.STREAM_WINDOW_SEC

    def seal_stream(self) -> None:
        """重启后气泡依附的连接已断，无法再刷，直接判定收尾。"""
        self.stream_done = True
        self.stream_deadline = 0.0

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, raw: dict) -> "TaskContext":
        return cls(**_only_known(cls, raw))


@dataclass
class Approval:
    """一张待审批的卡片。入站只需指向任务，出站还要带上待放行的回复内容。"""

    approval_id: str
    kind: str
    task_id: str
    content: str = ""
    media_id: str = ""

    @property
    def inbound(self) -> bool:
        return self.kind == KIND_INBOUND

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, raw: dict) -> "Approval":
        return cls(**_only_known(cls, raw))


@dataclass
class ReplyPayload:
    """Agent 通过 CLI 回传的一条结果。"""

    task_id: str
    content: str
    image: bytes | None = field(default=None, repr=False)
