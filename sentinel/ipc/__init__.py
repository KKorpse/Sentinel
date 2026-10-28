"""本机 CLI 与服务端之间的 IPC。

protocol  线上协议常量（无重依赖，CLI 单独引它即可）
server    unix socket 服务端
"""

__all__ = ["protocol", "server"]
