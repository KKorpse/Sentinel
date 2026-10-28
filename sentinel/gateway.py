"""业务编排。

入站：用户消息 → 气泡「等待确认」→ 送审 → 确认 → multica 建单派给 Agent
出站：Agent 调 CLI 回传 → 送审 → 确认 → 落回原会话（首条进气泡，第 N 条带标题另发）
追问：审批人单聊发言 → 作为评论续到目标任务，可用 `/{任务id}` 指定
"""

import asyncio
from pathlib import Path

from . import multica, prompts
from .config import Config, logger
from .ids import (new_approval_id, new_attachment_name, new_stream_id,
                  new_task_id)
from .models import KIND_INBOUND, KIND_OUTBOUND, Approval, ReplyPayload, TaskContext
from .store import Store
from .wecom import cards
from .wecom import media as media_util
from .wecom import message as msg
from .wecom import protocol as p
from .wecom.client import BotClient

LOG = logger("gateway")

# 气泡在各阶段的文案。{approver} 是审批人的展示名
TEXT_WAITING = "已收到，等待 {approver} 审核请求中..."
TEXT_APPROVING = "{approver} 审核通过，让我琢磨琢磨..."
TEXT_REVIEWING = "我搞定了，等 {approver} 审核结果..."
TEXT_REWORKING = "{approver} 对结果不满意，我继续琢磨琢磨..."
TEXT_REJECTED = "{approver} 拒绝了这条请求。"

# 审批人私聊里用 `/{任务id}` 指定追问目标，不带则沿用上一个任务
TARGET_TOKEN_PREFIX = "/"

# 回复序号：首条落在原消息气泡里，之后的脱离气泡、需自带任务标题
FIRST_REPLY_SEQ = 1
REPLY_HEADER = "**【{title}】** 追加回复 #{seq}"

CARD_TITLE_INBOUND = "有一条新的请求待你确认"
CARD_TITLE_OUTBOUND = "Agent 回复待你确认"
CARD_TITLE_APPROVED = "已确认"
CARD_TITLE_REJECTED = "已拒绝"
CARD_TITLE_FORBIDDEN = "无权处理该审批"
CARD_TITLE_EXPIRED = "该审批已失效"
CARD_TITLE_LOST = "原始请求已过期"

PLACEHOLDER_EMPTY = "-"
DIGEST_EMPTY = "无"


def split_target_token(text: str) -> tuple[str, str]:
    """拆出开头的 `/{任务id}` 指定前缀，返回 (标识, 剩余正文)。

    没带前缀时标识为空串。只认开头第一个 token，正文里的 `/` 当普通文本。
    """
    stripped = text.lstrip()
    if not stripped.startswith(TARGET_TOKEN_PREFIX):
        return "", text

    head, sep, rest = stripped[len(TARGET_TOKEN_PREFIX):].partition("\n")
    token, _, inline = head.partition(" ")
    if not token:
        return "", text
    return token.strip(), (inline + sep + rest).strip()


class Gateway:
    def __init__(self, config: Config, store: Store | None = None,
                 url: str = p.WS_URL):
        self.approver = config.approver_userid
        self.approver_display = config.approver_display
        self.assignee = config.multica_assignee
        self.store = store or Store()
        self.client = BotClient(
            bot_id=config.bot_id,
            secret=config.secret,
            on_callback=self._on_callback,
            url=url,
        )

    async def run(self, stop_event: asyncio.Event) -> None:
        await self.client.run(stop_event)

    # ---------- 消息落地 ----------

    async def _to_origin(self, task: TaskContext, body: dict) -> None:
        """回到最初发消息的会话：群聊回群，单聊回本人。"""
        if task.is_group:
            await self.client.push_to_chat(task.chatid, body)
            LOG.info("已送达群 %s", task.chatid)
        else:
            await self.client.push_to_user(task.sender, body)
            LOG.info("已送达用户 %s", task.sender)

    def _text(self, template: str) -> str:
        """气泡文案里把审批人称呼填进去。"""
        return template.format(approver=self.approver_display)

    async def _to_approver(self, body: dict) -> None:
        await self.client.push_to_user(self.approver, body)

    async def _notify_approver(self, text: str) -> None:
        await self._to_approver(cards.markdown(text))

    async def _refresh_stream(self, task: TaskContext, content: str,
                              finish: bool) -> bool:
        """刷新原消息的气泡；已收尾或超窗返回 False，由调用方决定如何兜底。"""
        if not task.stream_alive:
            return False
        await self.client.respond(
            task.callback_req_id, cards.stream(task.stream_id, content, finish))
        if finish:
            task.stream_done = True
        LOG.info("气泡已刷新 task_id=%s finish=%s", task.task_id, finish)
        return True

    async def _answer(self, task: TaskContext, content: str) -> None:
        """终态答复：气泡还活着就收尾在气泡里，否则另发一条。"""
        if not await self._refresh_stream(task, content, finish=True):
            await self._to_origin(task, cards.markdown(content))

    # ---------- 图片 ----------

    async def _first_image(self, body: dict) -> bytes | None:
        """取消息里第一张图并解密；url 只有 5 分钟有效期，收到就得取。"""
        for url, aeskey in msg.iter_image_sources(body):
            try:
                return await media_util.fetch(url, aeskey)
            except Exception:
                LOG.exception("下载/解密图片失败 url=%s", url)
        return None

    async def _upload(self, data: bytes, task_id: str) -> str:
        try:
            return await self.client.upload_image(data)
        except Exception:
            LOG.exception("上传图片失败 task_id=%s", task_id)
            return ""

    # ---------- 送审 ----------

    async def _ask_inbound(self, task: TaskContext) -> None:
        """把用户请求送审：先发原文（必要时连图），再发带按钮的卡片。"""
        approval = Approval(
            approval_id=new_approval_id(inbound=True),
            kind=KIND_INBOUND,
            task_id=task.task_id,
        )
        self.store.add_approval(approval)

        await self._notify_approver(
            f"**待确认请求** `{task.task_id}`\n"
            f"来自 {task.sender}（{task.chattype}）\n\n{task.request}")

        image = self.store.read_attachment(task.image_path)
        if image and not task.media_id:
            task.media_id = await self._upload(image, task.task_id)
            self.store.save()
        if task.media_id:
            await self._to_approver(cards.image(task.media_id))

        await self._to_approver(cards.template_card(cards.approval_card(
            approval.approval_id,
            CARD_TITLE_INBOUND,
            f"来自 {task.sender}（{task.chattype}）",
            task.request,
            [
                ("任务", task.task_id),
                ("发起人", task.sender),
                ("会话", task.chattype),
                ("类型", msg.type_name(task.msgtype)),
                ("图片", "有" if task.image_path else "无"),
            ],
        )))
        LOG.info("请求已送审 task_id=%s approval=%s",
                 task.task_id, approval.approval_id)

    async def _ask_outbound(self, task: TaskContext, reply: ReplyPayload) -> str:
        """把 Agent 回复送审。同一任务可多次调用，每次生成独立审批单。"""
        approval = Approval(
            approval_id=new_approval_id(inbound=False),
            kind=KIND_OUTBOUND,
            task_id=task.task_id,
            content=reply.content,
        )
        self.store.add_approval(approval)

        # Agent 交活了，气泡先提示"等审核结果"
        await self._refresh_stream(task, self._text(TEXT_REVIEWING), finish=False)

        await self._notify_approver(
            f"**待确认回复** `{task.task_id}`\n"
            f"将回给 {task.sender}（{task.chattype}）\n\n{reply.content}")

        if reply.image:
            approval.media_id = await self._upload(reply.image, task.task_id)
            self.store.save()
            if approval.media_id:
                await self._to_approver(cards.image(approval.media_id))

        await self._to_approver(cards.template_card(cards.approval_card(
            approval.approval_id,
            CARD_TITLE_OUTBOUND,
            f"任务 {task.task_id}",
            reply.content,
            [
                ("任务", task.task_id),
                ("单号", task.issue_id or PLACEHOLDER_EMPTY),
                ("回给", task.sender),
                ("会话", task.chattype),
                ("图片", "有" if reply.image else "无"),
            ],
        )))
        LOG.info("回复已送审 task_id=%s approval=%s",
                 task.task_id, approval.approval_id)
        self.store.mark_replied(task.task_id)
        return approval.approval_id

    # ---------- 审批放行后的动作 ----------

    async def _dispatch_to_agent(self, task: TaskContext) -> None:
        """入站放行：建 multica 任务并指派给 Agent。"""
        attachments = [Path(task.image_path)] if task.image_path else []
        try:
            issue_id = await multica.create_issue(
                title=prompts.issue_title(task.task_id),
                assignee=self.assignee,
                description=prompts.issue_description(task.task_id, task.request),
                attachments=attachments,
            )
        except Exception as e:
            LOG.exception("建单失败 task_id=%s", task.task_id)
            await self._notify_approver(f"任务 `{task.task_id}` 建单失败：{e}")
            return

        task.issue_id = issue_id
        self.store.save()
        LOG.info("已建单 task_id=%s issue=%s assignee=%s",
                 task.task_id, issue_id, self.assignee)
        await self._notify_approver(
            f"任务 `{task.task_id}` 已派给 **{self.assignee}**，单号 `{issue_id}`。")

    async def _reply_header(self, task: TaskContext) -> str:
        """第 N 条回复脱离了原气泡，开头补任务标题，方便对上是哪个任务。"""
        title = ""
        if task.issue_id:
            try:
                title = await multica.get_title(task.issue_id)
            except Exception:
                LOG.exception("取任务标题失败 issue=%s", task.issue_id)
        return REPLY_HEADER.format(title=title or task.request, seq=task.reply_seq)

    async def _deliver(self, task: TaskContext, approval: Approval) -> None:
        """出站放行：首条答复落在原消息气泡里，第 N 条带任务标题另发一条。"""
        task.reply_seq += 1

        landed = (task.reply_seq == FIRST_REPLY_SEQ
                  and await self._refresh_stream(task, approval.content, finish=True))
        if landed:
            LOG.info("首条答复已落到气泡 task_id=%s", task.task_id)
        else:
            header = await self._reply_header(task)
            await self._to_origin(
                task, cards.markdown(f"{header}\n\n{approval.content}"))
            LOG.info("第 %s 条答复已另发 task_id=%s", task.reply_seq, task.task_id)

        self.store.save()
        if approval.media_id:
            await self._to_origin(task, cards.image(approval.media_id))

    # ---------- 审批人追问 ----------

    def _digest_text(self) -> str:
        items = [f"`{t.task_id}`(`{t.issue_id}`)" for t in self.store.digest()]
        return "、".join(items) if items else DIGEST_EMPTY

    async def _switch_target(self, req_id: str, token: str) -> TaskContext | None:
        """处理 `/{任务id}` 指定：切换当前追问目标，切不动就回提示。"""
        task = self.store.resolve_task(token)
        if task is None:
            await self.client.respond(req_id, cards.markdown(
                f"找不到任务 `{token}`，可用任务：{self._digest_text()}"))
            return None
        if not task.dispatched:
            await self.client.respond(req_id, cards.markdown(
                f"任务 `{task.task_id}` 还没建单，无法追加评论。"))
            return None

        self.store.mark_replied(task.task_id)
        LOG.info("追问目标已切到 task_id=%s issue=%s", task.task_id, task.issue_id)
        return task

    async def _forward_as_comment(self, req_id: str, request: str,
                                  image: bytes | None) -> None:
        """审批人单聊发言：作为评论续到目标任务下，Agent 会继续处理。"""
        token, request = split_target_token(request)

        if token:
            task = await self._switch_target(req_id, token)
            if task is None:
                return
            if not request and image is None:
                await self.client.respond(req_id, cards.markdown(
                    f"已切到任务 `{task.task_id}`（单号 `{task.issue_id}`），"
                    f"接下来的发言都会追加到这个任务。"))
                return
        else:
            task = self.store.latest_dispatched()
            if task is None:
                await self.client.respond(req_id, cards.markdown(
                    "当前没有可追问的任务，请先在群里 @ 我发起一个任务。"))
                return

        attachments = []
        if image:
            attachments.append(self.store.save_attachment(
                new_attachment_name(task.task_id), image))

        try:
            await multica.add_comment(
                issue_id=task.issue_id,
                content=prompts.comment(task.task_id, request),
                attachments=attachments,
            )
        except Exception as e:
            LOG.exception("追加评论失败 task_id=%s issue=%s",
                          task.task_id, task.issue_id)
            await self.client.respond(req_id, cards.markdown(f"追加评论失败：{e}"))
            return

        LOG.info("已追加评论 task_id=%s issue=%s", task.task_id, task.issue_id)
        self.store.mark_replied(task.task_id)

        # 气泡已收尾过就刷不动了，此时这个提示只发在审批人单聊里
        await self._refresh_stream(task, self._text(TEXT_REWORKING), finish=False)
        await self.client.respond(req_id, cards.markdown(
            f"已作为评论追加到任务 `{task.task_id}`（单号 `{task.issue_id}`），"
            f"Agent 处理完会再送审。"))

    # ---------- 回调分发 ----------

    async def _on_message(self, req_id: str, body: dict) -> None:
        sender = p.sender_of(body)
        chattype = body.get(p.KEY_CHATTYPE, "")
        msgtype = body.get(p.KEY_MSGTYPE, "")
        content = msg.extract_content(body)
        quote_text = msg.format_quote(body.get(p.KEY_QUOTE))

        LOG.info("收到消息 from=%s chattype=%s chatid=%s msgtype=%s "
                 "content=%r quote=%r",
                 sender, chattype, body.get(p.KEY_CHATID, ""), msgtype,
                 content, quote_text)

        image = await self._first_image(body)
        if image is None and body.get(p.KEY_QUOTE):
            image = await self._first_image(body[p.KEY_QUOTE])

        # 审批人单聊发言视为对 Agent 追问；群里 @ 仍按新请求走审批
        if sender == self.approver and chattype == p.CHATTYPE_SINGLE:
            await self._forward_as_comment(
                req_id, msg.compose_request(content, quote_text), image)
            return

        task_id = new_task_id()
        task = TaskContext(
            task_id=task_id,
            sender=sender,
            chattype=chattype,
            chatid=body.get(p.KEY_CHATID, ""),
            msgid=body.get(p.KEY_MSGID, ""),
            msgtype=msgtype,
            content=content,
            quote=quote_text,
            request=msg.compose_request(content, quote_text),
            image_path=(str(self.store.save_attachment(task_id, image))
                        if image else ""),
        )
        task.open_stream(new_stream_id(), req_id)
        self.store.add_task(task)

        await self._refresh_stream(task, self._text(TEXT_WAITING), finish=False)
        await self._ask_inbound(task)

    async def _on_card_click(self, req_id: str, body: dict) -> None:
        event = p.event_of(body).get(p.EVENT_TEMPLATE_CARD) or {}
        approval_id = event.get(p.EVENT_TASK_ID, "")
        event_key = event.get(p.EVENT_KEY, "")
        clicker = p.sender_of(body)

        LOG.info("卡片点击 user=%s approval=%s event_key=%s",
                 clicker, approval_id, event_key)

        async def close(title: str, rows: list[tuple[str, str]],
                        detail: str = "") -> None:
            await self.client.respond_update(req_id, cards.update_template_card(
                cards.closed_card(approval_id, title, detail, rows, event_key)))

        if clicker != self.approver:
            LOG.warning("非审批人点击，忽略 user=%s", clicker)
            await close(CARD_TITLE_FORBIDDEN, [("操作人", clicker)])
            return

        approval = self.store.take_approval(approval_id)
        if approval is None:
            LOG.warning("审批单不存在或已处理 approval=%s", approval_id)
            await close(CARD_TITLE_EXPIRED, [("单据", approval_id)])
            return

        task = self.store.get_task(approval.task_id)
        if task is None:
            LOG.warning("任务上下文已淘汰 task_id=%s", approval.task_id)
            await close(CARD_TITLE_LOST, [("任务", approval.task_id)])
            return

        approved = event_key == cards.BUTTON_KEY_CONFIRM
        await close(
            CARD_TITLE_APPROVED if approved else CARD_TITLE_REJECTED,
            [("任务", task.task_id), ("操作", event_key)],
            approval.content or task.request,
        )

        if approval.inbound:
            if approved:
                self.client.spawn(
                    self._refresh_stream(task, self._text(TEXT_APPROVING), False))
                self.client.spawn(self._dispatch_to_agent(task))
            else:
                self.client.spawn(self._answer(task, self._text(TEXT_REJECTED)))
        elif approved:
            self.client.spawn(self._deliver(task, approval))
        else:
            LOG.info("回复被拒绝，不下发 task_id=%s", task.task_id)

    async def _on_enter_chat(self, req_id: str, body: dict) -> None:
        if p.sender_of(body) == self.approver:
            await self.client.respond(req_id, cards.markdown(
                f"直接发言会追加到上一个任务；用 `/任务id 内容` 可指定任务。\n"
                f"当前可指定：{self._digest_text()}"))
        else:
            await self.client.respond(req_id, cards.markdown(
                "你好，发消息给我，会先转给管理员确认后再处理。"))

    async def _on_event(self, req_id: str, body: dict) -> None:
        event_type = p.event_of(body).get(p.KEY_EVENTTYPE, "")
        LOG.info("收到事件 eventtype=%s from=%s", event_type, p.sender_of(body))

        if event_type == p.EVENT_TEMPLATE_CARD:
            await self._on_card_click(req_id, body)
        elif event_type == p.EVENT_ENTER_CHAT:
            await self._on_enter_chat(req_id, body)

    async def _on_callback(self, frame: dict) -> None:
        cmd = frame.get(p.KEY_CMD)
        req_id = p.frame_req_id(frame)
        body = p.frame_body(frame)
        try:
            if cmd == p.CMD_MSG_CALLBACK:
                await self._on_message(req_id, body)
            elif cmd == p.CMD_EVENT_CALLBACK:
                await self._on_event(req_id, body)
            else:
                LOG.warning("未处理的命令 cmd=%s", cmd)
        except Exception:
            LOG.exception("处理回调失败: %r", frame)

    # ---------- CLI 入口 ----------

    async def submit_reply(self, reply: ReplyPayload) -> TaskContext | None:
        """Agent 回传结果，落到出站审批。任务不存在返回 None。"""
        task = self.store.get_task(reply.task_id)
        if task is None:
            return None
        await self._ask_outbound(task, reply)
        return task
