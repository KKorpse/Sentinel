"""各类 ID 生成。

卡片 task_id 只允许数字/字母/_-@ 且 ≤128 字节，所以前缀都用安全字符。
"""

import uuid

REQ_ID_LEN = 32

TASK_ID_PREFIX = "SENT-"
TASK_ID_RAND_LEN = 12

APPROVAL_ID_PREFIX_INBOUND = "in_"
APPROVAL_ID_PREFIX_OUTBOUND = "out_"
APPROVAL_ID_RAND_LEN = 16

STREAM_ID_PREFIX = "stm_"
STREAM_ID_RAND_LEN = 16

ATTACHMENT_ID_RAND_LEN = 8


def _rand(length: int) -> str:
    return uuid.uuid4().hex[:length]


def new_req_id() -> str:
    """长连接每条请求的幂等标识。"""
    return _rand(REQ_ID_LEN)


def new_task_id() -> str:
    """Sentinel 任务 ID，贯穿审批、建单、回传全链路。"""
    return TASK_ID_PREFIX + _rand(TASK_ID_RAND_LEN)


def new_approval_id(inbound: bool) -> str:
    """审批单 ID，塞进卡片 task_id 字段，点击回调时据此找回上下文。"""
    prefix = APPROVAL_ID_PREFIX_INBOUND if inbound else APPROVAL_ID_PREFIX_OUTBOUND
    return prefix + _rand(APPROVAL_ID_RAND_LEN)


def new_stream_id() -> str:
    """流式气泡 ID，同一条消息的多次刷新必须复用同一个。"""
    return STREAM_ID_PREFIX + _rand(STREAM_ID_RAND_LEN)


def new_attachment_name(task_id: str) -> str:
    """同一任务可能多次带图，加随机后缀避免覆盖。"""
    return f"{task_id}_{_rand(ATTACHMENT_ID_RAND_LEN)}"
