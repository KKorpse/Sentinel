"""状态持久化与任务检索。

重启后要能恢复任务上下文与待办审批，否则审批人追问会找不到任务。
"""

import json
from pathlib import Path

from .config import ATTACHMENT_DIR, STATE_PATH, logger
from .models import Approval, TaskContext
from .wecom import media as media_util

LOG = logger("store")

FIELD_REQUESTS = "requests"
FIELD_APPROVALS = "approvals"
FIELD_LAST_REPLIED = "last_replied_task"

STATE_TMP_SUFFIX = ".tmp"

# 内存里保留的请求数上限，超出按先进先出淘汰
MAX_TRACKED_REQUESTS = 500
# 列给审批人看的候选任务个数
DIGEST_LIMIT = 5


class Store:
    def __init__(self, state_path: Path = STATE_PATH,
                 attachment_dir: Path = ATTACHMENT_DIR):
        self._state_path = state_path
        self._attachment_dir = attachment_dir
        self._attachment_dir.mkdir(parents=True, exist_ok=True)

        self._tasks: dict[str, TaskContext] = {}
        self._approvals: dict[str, Approval] = {}
        # 最近一次有交互的任务，审批人不指定目标时就续到这个任务下
        self._last_replied_task = ""
        self._load()

    # ---------- 落盘 / 恢复 ----------

    def _load(self) -> None:
        if not self._state_path.exists():
            return
        try:
            state = json.loads(self._state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            LOG.exception("状态文件损坏，忽略 %s", self._state_path)
            return

        self._tasks = {
            task_id: TaskContext.from_dict(raw)
            for task_id, raw in state.get(FIELD_REQUESTS, {}).items()
        }
        self._approvals = {
            approval_id: Approval.from_dict({"approval_id": approval_id, **raw})
            for approval_id, raw in state.get(FIELD_APPROVALS, {}).items()
        }
        self._last_replied_task = state.get(FIELD_LAST_REPLIED, "")

        for task in self._tasks.values():
            task.seal_stream()

        LOG.info("已恢复状态 tasks=%s approvals=%s last=%s",
                 len(self._tasks), len(self._approvals), self._last_replied_task)

    def save(self) -> None:
        """全量落盘。量级是几百条纯文本，整写比增量更不容易出一致性问题。"""
        state = {
            FIELD_REQUESTS: {k: v.to_dict() for k, v in self._tasks.items()},
            FIELD_APPROVALS: {k: v.to_dict() for k, v in self._approvals.items()},
            FIELD_LAST_REPLIED: self._last_replied_task,
        }
        tmp = self._state_path.with_suffix(STATE_TMP_SUFFIX)
        try:
            tmp.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")
            tmp.replace(self._state_path)  # 原子替换，避免半截文件
        except OSError:
            LOG.exception("状态落盘失败 %s", self._state_path)

    # ---------- 任务 ----------

    def add_task(self, task: TaskContext) -> None:
        while len(self._tasks) >= MAX_TRACKED_REQUESTS:
            self._tasks.pop(next(iter(self._tasks)))
        self._tasks[task.task_id] = task
        self.save()

    def get_task(self, task_id: str) -> TaskContext | None:
        return self._tasks.get(task_id)

    def resolve_task(self, token: str) -> TaskContext | None:
        """按标识找任务：先按 Sentinel 任务 ID，再按 multica 单号。"""
        task = self._tasks.get(token)
        if task is not None:
            return task
        return next((t for t in self._tasks.values() if t.issue_id == token), None)

    def latest_dispatched(self) -> TaskContext | None:
        """默认追问目标：优先最近一次有交互的，否则取最近一个已建单的。"""
        task = self._tasks.get(self._last_replied_task)
        if task is not None and task.dispatched:
            return task
        return next((t for t in reversed(list(self._tasks.values())) if t.dispatched),
                    None)

    def digest(self, limit: int = DIGEST_LIMIT) -> list[TaskContext]:
        """最近若干个已建单的任务，供审批人指定时对号。"""
        return [t for t in reversed(list(self._tasks.values())) if t.dispatched][:limit]

    # ---------- 最近任务指针 ----------

    @property
    def last_replied_task(self) -> str:
        return self._last_replied_task

    def mark_replied(self, task_id: str) -> None:
        self._last_replied_task = task_id
        self.save()

    # ---------- 审批单 ----------

    def add_approval(self, approval: Approval) -> None:
        self._approvals[approval.approval_id] = approval
        self.save()

    def take_approval(self, approval_id: str) -> Approval | None:
        """取出并消费掉，保证同一张卡片不会被点两次。"""
        approval = self._approvals.pop(approval_id, None)
        if approval is not None:
            self.save()
        return approval

    # ---------- 附件 ----------

    def save_attachment(self, name: str, data: bytes) -> Path:
        path = self._attachment_dir / f"{name}{media_util.guess_ext(data)}"
        path.write_bytes(data)
        return path

    @staticmethod
    def read_attachment(path_str: str) -> bytes | None:
        if not path_str:
            return None
        path = Path(path_str)
        if not path.is_file():
            LOG.warning("附件已丢失 %s", path)
            return None
        return path.read_bytes()
