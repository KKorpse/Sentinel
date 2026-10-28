"""媒体资源的解密、下载与类型识别。

回调里的 image/file/video 只给一个 5 分钟有效的临时 url，内容是加密的，
必须用同结构体里的 aeskey 解密。
"""

import asyncio
import base64
import urllib.request

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

# AES-256-CBC；IV 取密钥前 16 字节；明文 PKCS#7 补到 32 字节倍数
AES_KEY_LEN = 32
AES_IV_LEN = 16
AES_BLOCK_SIZE = 32
BASE64_GROUP_LEN = 4

IMAGE_MAGIC_EXT = (
    (b"\x89PNG\r\n\x1a\n", ".png"),
    (b"\xff\xd8\xff", ".jpg"),
    (b"GIF87a", ".gif"),
    (b"GIF89a", ".gif"),
)
DEFAULT_IMAGE_EXT = ".png"
DEFAULT_IMAGE_FILENAME = "image.png"


def derive_key(aeskey: str) -> bytes:
    """把回调里的 aeskey 归一化成 32 字节密钥。

    官方给的是 base64，现网出现过去掉 '=' 的 43 字符形态，需要自己补齐；
    同时兼容直接给 32 字节明文密钥的情况。
    """
    raw = aeskey.encode("utf-8")
    if len(raw) == AES_KEY_LEN:
        return raw
    padded = aeskey + "=" * (-len(aeskey) % BASE64_GROUP_LEN)
    key = base64.b64decode(padded)
    if len(key) != AES_KEY_LEN:
        raise ValueError(f"aeskey 解码后应为 {AES_KEY_LEN} 字节，实际 {len(key)}")
    return key


def decrypt(data: bytes, aeskey: str) -> bytes:
    key = derive_key(aeskey)
    decryptor = Cipher(algorithms.AES(key), modes.CBC(key[:AES_IV_LEN])).decryptor()
    plain = decryptor.update(data) + decryptor.finalize()
    pad = plain[-1]
    if not 1 <= pad <= AES_BLOCK_SIZE:
        raise ValueError(f"非法 PKCS#7 填充值 {pad}")
    return plain[:-pad]


async def fetch(url: str, aeskey: str) -> bytes:
    """下载并解密。阻塞操作丢到线程池，别卡住事件循环。"""

    def _download() -> bytes:
        with urllib.request.urlopen(url) as resp:
            return resp.read()

    raw = await asyncio.to_thread(_download)
    if not aeskey:
        return raw
    return await asyncio.to_thread(decrypt, raw, aeskey)


def guess_ext(data: bytes) -> str:
    for magic, ext in IMAGE_MAGIC_EXT:
        if data.startswith(magic):
            return ext
    return DEFAULT_IMAGE_EXT
