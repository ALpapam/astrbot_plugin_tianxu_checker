# 天絮API检测核心（纯 asyncio + aiohttp，不依赖 AstrBot，可独立测试）
#
# 与网页版 / Node 服务器版共用同一套逻辑：
# - 模型列表：/v1/models（带 Key）∪ /api/pricing 全部模型名，去重并集（运行时自动合并，永不遗漏）
# - 并发检测 + 全局限速：串行分发槽位（min-gap = 60000/rpm ms），保证任意 60s 窗口内启动数 ≤ rpm
# - 首字延迟（TTFB）：流式读取，测量到首个 content / reasoning / tool_calls / finish_reason
#   （聊天）/ data（embedding）/ results（rerank）为止
# - 分类：ms ≤ green_ms → 可用（green）；ms ≤ yellow_ms → 一般（yellow）；否则不可用（red）
# - 错误：HTTP 状态码 + error.message；网关在 ~126s 中断（Cloudflare 524）单独识别
import asyncio
import json
import time
from typing import Dict, List, Optional, Tuple

import aiohttp


def fmt_ms(ms: int) -> str:
    """格式化毫秒：<1s 显示 ms，<60s 显示 x.ys，否则显示 xmys。"""
    ms = max(0, int(ms))
    if ms < 1000:
        return f"{ms}ms"
    s = ms // 1000
    if s < 60:
        return f"{s}.{ms % 1000 // 100}s"
    m, s2 = divmod(s, 60)
    return f"{m}m{s2}s"


def _nonempty(v) -> bool:
    if v is None:
        return False
    if isinstance(v, str):
        return v != ""
    if isinstance(v, (list, dict)):
        return len(v) > 0
    return True


def _err_text(err) -> str:
    if isinstance(err, dict):
        msg = err.get("message") or err.get("type") or json.dumps(err, ensure_ascii=False)
        return str(msg)[:200]
    return str(err)[:200]


def inspect_payload(obj) -> Tuple[bool, Optional[str]]:
    """从一次 JSON 响应判断是否已产生真实输出，或是否携带错误。

    返回 (got_output, error_message)。got_output=True 即视为首字已到。
    """
    if not isinstance(obj, dict):
        return False, None
    err = obj.get("error")
    if err:
        return False, _err_text(err)
    choices = obj.get("choices")
    if isinstance(choices, list):
        for c in choices:
            if not isinstance(c, dict):
                continue
            if c.get("finish_reason"):
                return True, None
            m = c.get("message") or c.get("delta")
            if isinstance(m, dict):
                for k in ("content", "reasoning_content", "reasoning", "tool_calls"):
                    if _nonempty(m.get(k)):
                        return True, None
    # 非聊天（embedding / rerank）
    if _nonempty(obj.get("data")) or _nonempty(obj.get("results")):
        return True, None
    return False, None


def build_listing_text(header: str, available: List[Tuple[str, int]], footer: str = "") -> str:
    """生成公布文本：标题 + No.序号 模型名 + ms 延迟（按延迟升序） + 可选页脚。"""
    lines = [header or "天絮API目前可用模型"]
    for i, (mid, ms) in enumerate(available, 1):
        lines.append(f"No.{i} {mid}")
        lines.append(f"{ms}ms")
    if footer:
        lines.append(footer)
    return "\n".join(lines)


def chunk_text(text: str, limit: int = 1500) -> List[str]:
    """按行分片，每片不超过 limit 字符（尽量在行边界断开）。"""
    chunks: List[str] = []
    cur = ""
    for line in text.split("\n"):
        if cur and len(cur) + len(line) + 1 > limit:
            chunks.append(cur)
            cur = ""
        cur = (cur + "\n" + line) if cur else line
    if cur:
        chunks.append(cur)
    return chunks


class ApiTester:
    def __init__(self, config: dict):
        self.config = config
        self._session: Optional[aiohttp.ClientSession] = None
        self._slot_lock = asyncio.Lock()
        self._last_slot_ms = 0.0

    # ---------- 配置读取 ----------
    def _c(self, key, default=None):
        return self.config.get(key, default)

    @property
    def base_url(self) -> str:
        return str(self._c("base_url", "https://chat-api4.087654.xyz")).rstrip("/")

    @property
    def api_key(self) -> str:
        return str(self._c("api_key", ""))

    @property
    def rpm(self) -> int:
        return max(1, int(self._c("rpm", 10)))

    @property
    def max_concurrency(self) -> int:
        return max(1, int(self._c("max_concurrency", 10)))

    @property
    def timeout_ms(self) -> int:
        return max(1000, int(self._c("timeout_ms", 1000000)))

    @property
    def green_ms(self) -> int:
        return max(1, int(self._c("green_ms", 60000)))

    @property
    def yellow_ms(self) -> int:
        return max(self.green_ms, int(self._c("yellow_ms", 300000)))

    @property
    def strict(self) -> bool:
        return bool(self._c("strict", True))

    @property
    def include_yellow(self) -> bool:
        return bool(self._c("include_yellow", False))

    @property
    def prompt(self) -> str:
        return str(self._c("prompt", "你好，请回复一个字符。"))

    # ---------- 会话 ----------
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            # trust_env=True：读取 HTTP(S)_PROXY 环境变量（本地测试走代理；服务器无代理时无副作用）
            self._session = aiohttp.ClientSession(trust_env=True)
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    # ---------- RPM 限速（串行槽位分发） ----------
    async def _acquire_slot(self):
        async with self._slot_lock:
            gap = 60000.0 / self.rpm
            now = time.time() * 1000
            target = max(now, self._last_slot_ms + gap)
            if target > now:
                await asyncio.sleep((target - now) / 1000.0)
            self._last_slot_ms = time.time() * 1000

    # ---------- 模型列表 ----------
    async def load_model_ids(self) -> List[str]:
        ids: set = set()
        session = await self._get_session()
        headers = {"Authorization": "Bearer " + self.api_key} if self.api_key else {}
        try:
            async with session.get(
                self.base_url + "/v1/models",
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as r:
                if r.status == 200:
                    j = await r.json(content_type=None)
                    for m in j.get("data") or []:
                        mid = m.get("id") if isinstance(m, dict) else m
                        if mid:
                            ids.add(str(mid))
        except Exception as e:
            print(f"[tianxu] /v1/models 获取失败: {e}")
        try:
            async with session.get(
                self.base_url + "/api/pricing",
                timeout=aiohttp.ClientTimeout(total=15),
            ) as r:
                if r.status == 200:
                    j = await r.json(content_type=None)
                    for m in j.get("data") or []:
                        if isinstance(m, dict) and m.get("model_name"):
                            ids.add(str(m["model_name"]))
        except Exception as e:
            print(f"[tianxu] /api/pricing 获取失败: {e}")
        return sorted(ids)

    # ---------- 单模型检测 ----------
    @staticmethod
    def kind_of(model_id: str) -> str:
        s = model_id.lower()
        if "rerank" in s:
            return "rerank"
        if "embed" in s or s.startswith("bge-"):
            return "embed"
        return "chat"

    def body_for(self, model_id: str, kind: str) -> dict:
        if kind == "rerank":
            return {
                "model": model_id,
                "query": self.prompt,
                "documents": ["你好", "今天天气不错"],
            }
        if kind == "embed":
            return {"model": model_id, "input": self.prompt}
        return {
            "model": model_id,
            "messages": [{"role": "user", "content": self.prompt}],
            "stream": True,
        }

    async def _consume_stream(self, resp, kind: str, t0: float):
        """流式读取直到首字/错误。返回 (got, errmsg, ms_at_got, interrupted)。"""
        got = False
        errmsg: Optional[str] = None
        ms_at_got: Optional[float] = None
        interrupted = False
        buf = ""
        try:
            async for chunk in resp.content:
                if not chunk:
                    continue
                buf += chunk.decode("utf-8", errors="ignore")
                if kind != "chat":
                    try:
                        obj = json.loads(buf)
                        out, err = inspect_payload(obj)
                        if err:
                            errmsg = err
                            break
                        if out:
                            got = True
                            ms_at_got = time.time() * 1000 - t0
                            break
                    except Exception:
                        pass
                else:
                    while "\n" in buf:
                        line, buf = buf.split("\n", 1)
                        line = line.strip()
                        if not line or line.startswith(":"):
                            continue
                        if line.startswith("data:"):
                            line = line[5:].strip()
                        if line == "[DONE]":
                            continue
                        try:
                            obj = json.loads(line)
                            out, err = inspect_payload(obj)
                            if err:
                                errmsg = err
                                break
                            if out:
                                got = True
                                ms_at_got = time.time() * 1000 - t0
                                break
                        except Exception:
                            pass
                    if errmsg or got:
                        break
        except (aiohttp.ClientPayloadError, aiohttp.ClientConnectionError,
                aiohttp.ServerDisconnectedError, asyncio.TimeoutError, ConnectionError):
            interrupted = True
        except Exception:
            interrupted = True
        # 兜底：部分模型忽略 stream 参数，直接返回一段完整 JSON（可能无换行）
        if not got and not errmsg and buf.strip():
            try:
                obj = json.loads(buf)
                out, err = inspect_payload(obj)
                if err:
                    errmsg = err
                elif out:
                    got = True
                    ms_at_got = time.time() * 1000 - t0
            except Exception:
                pass
        return got, errmsg, ms_at_got, interrupted

    async def test_one(self, model_id: str) -> dict:
        kind = self.kind_of(model_id)
        if kind == "rerank":
            url = self.base_url + "/v1/rerank"
        elif kind == "embed":
            url = self.base_url + "/v1/embeddings"
        else:
            url = self.base_url + "/v1/chat/completions"
        body = self.body_for(model_id, kind)
        headers = {
            "Content-Type": "application/json",
            "Accept": "text/event-stream, application/json",
        }
        if self.api_key:
            headers["Authorization"] = "Bearer " + self.api_key

        res = {"s": "red", "ms": None, "n": "", "h": 0}
        t0 = time.time() * 1000
        session = await self._get_session()
        try:
            async with session.post(
                url,
                json=body,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=self.timeout_ms / 1000.0),
            ) as resp:
                res["h"] = resp.status
                if resp.status != 200:
                    res["ms"] = int(time.time() * 1000 - t0)
                    try:
                        txt = await resp.text()
                    except Exception:
                        txt = ""
                    res["n"] = self._error_text(txt, resp.status)
                    return res
                got, errmsg, ms_at_got, interrupted = await self._consume_stream(resp, kind, t0)
                elapsed = int(time.time() * 1000 - t0)
                res["ms"] = int(ms_at_got) if got and ms_at_got is not None else elapsed
                if errmsg:
                    res["n"] = errmsg
                elif got:
                    res["s"] = self.classify(res["ms"])
                    if res["s"] == "red":
                        res["n"] = "响应过慢"
                else:
                    if interrupted and elapsed > 100000:
                        res["n"] = f"超时无响应：网关在 {fmt_ms(elapsed)} 处中断连接（Cloudflare 524）"
                    else:
                        res["n"] = "未收到模型回复"
                return res
        except asyncio.TimeoutError:
            res["ms"] = int(time.time() * 1000 - t0)
            res["n"] = f"超时 {fmt_ms(self.timeout_ms)} 无响应"
            return res
        except Exception as e:
            res["ms"] = int(time.time() * 1000 - t0)
            res["n"] = f"连接失败（{type(e).__name__}）"
            return res

    def classify(self, ms: int) -> str:
        if ms <= self.green_ms:
            return "green"
        if ms <= self.yellow_ms:
            return "yellow"
        return "red"

    @staticmethod
    def _error_text(txt: str, status: int) -> str:
        try:
            j = json.loads(txt)
            msg = _err_text(j.get("error"))
            if msg:
                return f"HTTP {status}: {msg}"
        except Exception:
            pass
        t = txt.strip().replace("\n", " ")[:120]
        return f"HTTP {status}: {t}" if t else f"HTTP {status}"

    # ---------- 全量检测 ----------
    async def run_all(self, limit: Optional[int] = None, ids: Optional[List[str]] = None) -> dict:
        """检测全部模型。limit / ids 仅供测试。返回 summary。"""
        if ids is None:
            ids = await self.load_model_ids()
        if limit:
            ids = ids[:limit]
        if not ids:
            return {"ok": False, "ids": [], "results": {}, "available": [], "duration": 0, "counts": {}}

        sem = asyncio.Semaphore(self.max_concurrency)
        results: Dict[str, dict] = {}

        async def work(model_id: str):
            await self._acquire_slot()
            async with sem:
                results[model_id] = await self.test_one(model_id)

        t0 = time.time()
        await asyncio.gather(*(work(i) for i in ids))
        duration = time.time() - t0

        counts = {"green": 0, "yellow": 0, "red": 0}
        for r in results.values():
            counts[r["s"]] = counts.get(r["s"], 0) + 1

        available = []
        for i in ids:
            r = results[i]
            if r["ms"] is None:
                continue
            if r["s"] == "green":
                available.append((i, r["ms"]))
            elif self.include_yellow and r["s"] == "yellow":
                available.append((i, r["ms"]))
        available.sort(key=lambda x: x[1])

        return {
            "ok": True,
            "ids": ids,
            "results": results,
            "available": available,
            "duration": duration,
            "counts": counts,
        }
