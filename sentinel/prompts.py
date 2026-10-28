"""派给 Agent 的提示词模板。

改文案只需要动这个文件，不用碰业务逻辑。
"""

from .config import CLI_PATH
from .multica import BIN as MULTICA_BIN

ISSUE_TITLE = "Sentinel 待处理任务 {task_id}"

_UNDERSTAND = (
    "收到该任务后，先理解任务，"
    "然后优先搜一下当前卡有没有一模一样的任务或者是问题，有的话直接参考原对应的结果即可，"
    "再执行该任务。"
)

_TITLE_RULE = f"""\
## 必做：执行完后改标题

任务执行完成后，**必须**把本任务的标题改成符合任务内容的标题，不要留着建单时的占位标题。
按任务性质从下面三种前缀里选一种，冒号后面用一句话概括：

- `【问题】：一句话描述问题`
- `【需求】：一句话描述需求`
- `【其他】：一句话描述内容`

改标题的命令（`<本任务编号>` 换成本任务的编号，如 WS-328）：

```bash
{MULTICA_BIN} issue update <本任务编号> --title "【问题】：一句话描述问题"
```"""

_CLI_USAGE = """\
## 必做：把结果回传给 Sentinel Cli

改完标题后，把结果发送给 Sentinel Cli，带上任务 id：{task_id}

```bash
python3 {cli_path} reply --task-id {task_id} --content-stdin <<'EOF'
任务 id：{task_id}

这里写要回复给用户的内容
EOF
```

要求：
1. **回复正文里必须带上任务 id `{task_id}`**，放在开头一行，方便提问人对上是哪个任务。
2. 如需回图片，追加 `--image <本地图片路径>`。
3. **本任务下每新增一条评论，你处理完之后都必须再调用一次上面的命令**
   （任务 id 始终是 {task_id}）。不要只在首次执行后调用一次，也不要因为是追加评论就跳过，
   每一轮都要发，否则提问的人收不到你的回复。
4. 每一轮处理完，如果标题已不能准确概括当前任务内容，要按上面的格式重新改一次标题。"""

_DESCRIPTION = """\
以下内容来自企业微信用户，请按原文理解：

<原始消息>
{request}
</原始消息>

{understand}

{title_rule}

{usage}"""

_COMMENT = f"""\
以下是提问人的追加消息，请按原文理解并继续处理：

<追加消息>
{{content}}
</追加消息>

处理完成后：

1. 如果现有标题已不能准确概括当前任务内容，按
   `【问题】：…` / `【需求】：…` / `【其他】：…` 的格式重新改一次标题：
   `{MULTICA_BIN} issue update <本任务编号> --title "…"`
2. **务必**再调用一次 Sentinel Cli 回传结果，回复正文开头要带上任务 id `{{task_id}}`：

```bash
python3 {{cli_path}} reply --task-id {{task_id}} --content-stdin <<'EOF'
任务 id：{{task_id}}

这里写要回复给用户的内容
EOF
```"""


def issue_title(task_id: str) -> str:
    """建单时的占位标题，Agent 执行完会按规则改掉。"""
    return ISSUE_TITLE.format(task_id=task_id)


def issue_description(task_id: str, request: str) -> str:
    return _DESCRIPTION.format(
        request=request,
        understand=_UNDERSTAND,
        title_rule=_TITLE_RULE,
        usage=_CLI_USAGE.format(cli_path=CLI_PATH, task_id=task_id),
    )


def comment(task_id: str, content: str) -> str:
    return _COMMENT.format(content=content, task_id=task_id, cli_path=CLI_PATH)
