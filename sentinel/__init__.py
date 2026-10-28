"""Sentinel —— 人工审批网关。

包结构：
  config    配置与路径
  ids       各类 ID 生成
  models    领域模型（任务上下文、审批单）
  store     状态持久化
  prompts   派给 Agent 的提示词模板
  multica   multica CLI 封装
  gateway   业务编排
  ipc       本机 CLI 服务
  wecom/    企业微信机器人协议层（协议常量、消息解析、媒体、卡片、长连接客户端）
"""

__all__ = ["config", "gateway", "ids", "ipc", "models", "multica", "prompts",
           "store", "wecom"]
