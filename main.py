"""天絮API可用模型检测插件（AstrBot v4 / Star API）。

功能：
- 定时（默认北京时间 :00/:30，自 00:00 起每 30 分钟）在机器人所在服务器上检测
  天絮API全部模型的连通性与首字延迟（全局限速 rpm 次/分钟，流式 TTFB）。
- 检测完全结束后，将可用模型按首字延迟升序排名（No.1 延迟最小），只公布到群白名单。
- 公布方式可由管理员选择：普通文本（text，通用）或群聊合并转发（forward，OneBot v11）。
- 手动触发：白名单群成员或管理员在群里发「状态」（不带斜杠也生效）立即触发一轮检测；
  管理员用 /mc_run 也可立即触发。
- 其它命令：/mc_status（查看上次结果）、/mc_mode text|forward（切换公布方式，管理员）。
"""
import asyncio
import time
from datetime import datetime
from typing import Optional

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.message_components import Node, Nodes, Plain
from astrbot.api.star import Context, Star
from astrbot.core import AstrBotConfig

from .core.tester import ApiTester, build_listing_text, chunk_text, fmt_ms

try:  # apscheduler 为 AstrBot 自带依赖；极少数环境缺失时仅停用定时、保留手动命令
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.triggers.cron import CronTrigger
    import zoneinfo

    _HAS_APS = True
except Exception:
    _HAS_APS = False


class TianxuApiChecker(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self.tester = ApiTester(dict(config))
        self._scheduler: Optional[AsyncIOScheduler] = None
        self._running = False
        self._platform_ids = set()  # 从收到的消息事件中捕获的 platform_id
        self.last_run: Optional[dict] = None

    def _cfg(self, key, default=None):
        return self.config.get(key, default)

    # ---------- 生命周期 ----------
    async def initialize(self):
        if _HAS_APS and self._scheduler is None:
            try:
                tz = zoneinfo.ZoneInfo(self._cfg("timezone", "Asia/Shanghai"))
            except Exception:
                tz = None
            self._scheduler = AsyncIOScheduler(timezone=tz)
            self._scheduler.add_job(
                self._scheduled_broadcast,
                CronTrigger(minute="0,30"),
                id="tianxu_api_checker",
                max_instances=1,
                coalesce=True,
            )
            self._scheduler.start()
            logger.info(
                "[天絮检测] 定时任务已启动：北京时间 :00/:30（自 00:00 起每 30 分钟）自动检测并公布到白名单群"
            )

    async def terminate(self):
        if self._scheduler:
            try:
                self._scheduler.shutdown(wait=False)
            except Exception:
                pass
            self._scheduler = None
        await self.tester.close()

    # ---------- 定时触发 ----------
    async def _scheduled_broadcast(self):
        await self._run_and_broadcast(source="定时")

    async def _run_and_broadcast(self, source: str, notify_umo: Optional[str] = None):
        if self._running:
            logger.info("[天絮检测] 上一轮检测仍在进行，本轮跳过")
            return
        self._running = True
        # 重新读取配置（WebUI 修改即时生效）
        self.tester = ApiTester(dict(self.config))
        try:
            summary = await self.tester.run_all()
            self.last_run = summary
            if not summary.get("ok"):
                logger.error("[天絮检测] 本轮检测失败：无法获取模型列表")
                return
            await self._broadcast(summary, source)
            # 手动触发（私聊/非白名单群）时，把结果也回发给发起会话
            if notify_umo:
                try:
                    parts = notify_umo.split(":")
                    gid = parts[2] if len(parts) >= 3 and parts[1] == "GroupMessage" else None
                except Exception:
                    gid = None
                if gid not in self._whitelist():
                    await self._send_text(notify_umo, self._build_listing(summary))
        except Exception as e:
            logger.exception(f"[天絮检测] 检测异常: {e}")
        finally:
            self._running = False

    # ---------- 公布 ----------
    def _whitelist(self) -> list:
        out = []
        for g in self._cfg("group_whitelist", []) or []:
            s = str(g).strip()
            if s.isdigit():
                out.append(s)
        return out

    def _resolved_platform_ids(self) -> list:
        pids = set(self._platform_ids)
        pids.add("aiocqhttp")  # 默认 OneBot v11
        try:
            mgr = getattr(self.context, "platform_manager", None)
            for p in getattr(mgr, "platform_insts", None) or []:
                try:
                    mid = p.meta().id
                    if mid:
                        pids.add(mid)
                except Exception:
                    pass
        except Exception:
            pass
        return sorted(pids)

    def _bj_now_str(self) -> str:
        try:
            now = datetime.now(zoneinfo.ZoneInfo("Asia/Shanghai"))
        except Exception:
            now = datetime.now()
        return now.strftime("%Y-%m-%d %H:%M")

    def _build_listing(self, summary) -> str:
        available = summary.get("available") or []
        header = str(self._cfg("announce_header", "天絮API目前可用模型") or "天絮API目前可用模型")
        dur = fmt_ms(int((summary.get("duration") or 0) * 1000))
        footer = f"（共 {len(available)} 个可用 · 北京时间 {self._bj_now_str()} · 检测用时 {dur}）"
        return build_listing_text(header, available, footer)

    async def _broadcast(self, summary, source: str):
        groups = self._whitelist()
        if not groups:
            logger.warning(f"[天絮检测] 未配置群白名单，本轮({source})仅检测不公布")
            return
        mode = str(self._cfg("send_mode", "text"))
        pids = self._resolved_platform_ids()
        available = summary.get("available") or []
        total = len(summary.get("ids") or [])
        counts = summary.get("counts") or {}
        listing = self._build_listing(summary)
        logger.info(
            f"[天絮检测] 公布：可用 {len(available)}/{total}（green {counts.get('green', 0)}/"
            f"yellow {counts.get('yellow', 0)}/red {counts.get('red', 0)}），"
            f"模式={mode}，群={groups}，平台={pids}，来源={source}"
        )
        for gid in groups:
            sent = False
            for pid in pids:
                umo = f"{pid}:GroupMessage:{gid}"
                try:
                    if mode == "forward" and str(pid).startswith("aiocqhttp"):
                        await self._send_forward(umo, available, listing)
                    else:
                        await self._send_text(umo, listing)
                    sent = True
                    break
                except Exception as e:
                    logger.warning(f"[天絮检测] 发送到 {umo} 失败: {e}")
            if not sent:
                logger.error(f"[天絮检测] 未能发送到群 {gid}")

    async def _send_text(self, umo: str, listing: str):
        for chunk in chunk_text(listing, 1500):
            await self.context.send_message(umo, MessageChain([Plain(chunk)]))

    async def _send_forward(self, umo: str, available, listing: str):
        maxn = max(1, int(self._cfg("max_models_per_broadcast", 50)))
        name = str(self._cfg("bot_display_name", "天絮API检测") or "天絮API检测")
        try:
            uin = int(self._cfg("bot_uin", "0") or 0)
        except Exception:
            uin = 0
        header = str(self._cfg("announce_header", "天絮API目前可用模型") or "天絮API目前可用模型")
        nodes = [Node(name=name, uin=uin, content=[Plain(header)])]
        for i, (mid, ms) in enumerate(available[:maxn], 1):
            nodes.append(Node(name=name, uin=uin, content=[Plain(f"No.{i} {mid}\n{ms}ms")]))
        if len(available) > maxn:
            nodes.append(
                Node(name=name, uin=uin,
                     content=[Plain(f"… 其余 {len(available) - maxn} 个略过（共 {len(available)} 个可用）")])
            )
        else:
            footer = listing.rsplit("\n", 1)[-1]
            if footer and footer.startswith("（共"):
                nodes.append(Node(name=name, uin=uin, content=[Plain(footer)]))
        await self.context.send_message(umo, MessageChain([Nodes(nodes)]))

    # ---------- 命令 ----------
    @filter.regex(r"^/?状态[。！!]?$")
    async def on_status_trigger(self, event: AstrMessageEvent):
        """在群里发『状态』立即触发一次检测（白名单群成员或管理员；regex 不受唤醒词限制）。"""
        self._platform_ids.add(event.get_platform_id())
        gid = event.get_group_id()
        is_admin = event.is_admin()
        wl = self._whitelist()
        if not is_admin and (gid is None or str(gid) not in wl):
            yield event.plain_result("仅管理员或白名单群可触发检测。")
            return
        if self._running:
            yield event.plain_result("检测正在进行中，请稍候（每 30 分钟自动一轮）。")
            return
        yield event.plain_result(
            "开始检测全部模型（约 13-16 分钟），完成后自动公布到白名单群。"
        )
        asyncio.create_task(
            self._run_and_broadcast(source="状态触发", notify_umo=event.unified_msg_origin)
        )

    @filter.command("mc_run")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def mc_run(self, event: AstrMessageEvent):
        """立即检测全部模型，完成后公布到白名单群（管理员）。"""
        self._platform_ids.add(event.get_platform_id())
        if self._running:
            yield event.plain_result("检测正在进行中，请稍候（每 30 分钟自动一轮）。")
            return
        rpm = self._cfg("rpm", 10)
        yield event.plain_result(
            f"开始检测全部模型（全局限速 {rpm} 次/分钟，约 13-16 分钟），完成后自动公布到白名单群。"
        )
        asyncio.create_task(
            self._run_and_broadcast(source="手动", notify_umo=event.unified_msg_origin)
        )

    @filter.command("mc_status")
    async def mc_status(self, event: AstrMessageEvent):
        """查看最近一次检测结果。"""
        self._platform_ids.add(event.get_platform_id())
        if self._running:
            yield event.plain_result("检测正在进行中…（每 30 分钟自动一轮）")
            return
        if not self.last_run:
            yield event.plain_result("还没有检测记录。每 30 分钟自动检测一次；也可由管理员用 /mc_run 立即触发。")
            return
        s = self.last_run
        av = s.get("available") or []
        total = len(s.get("ids") or [])
        counts = s.get("counts") or {}
        dur = fmt_ms(int((s.get("duration") or 0) * 1000))
        top = "\n".join(f"No.{i + 1} {mid} {ms}ms" for i, (mid, ms) in enumerate(av[:3]))
        yield event.plain_result(
            f"上次检测（北京时间 {self._bj_now_str()}）：共 {total} 个模型，"
            f"可用 {len(av)} 个（green {counts.get('green', 0)}/yellow {counts.get('yellow', 0)}/"
            f"red {counts.get('red', 0)}），用时 {dur}。\n"
            f"最快：\n{top}"
        )

    @filter.command("mc_mode")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def mc_mode(self, event: AstrMessageEvent, mode: str = ""):
        """切换公布方式：/mc_mode text（普通文本）或 /mc_mode forward（群聊合并转发，管理员）。"""
        self._platform_ids.add(event.get_platform_id())
        mode = (mode or "").strip().lower()
        if mode not in ("text", "forward"):
            yield event.plain_result(
                f"用法：/mc_mode text（普通文本）或 /mc_mode forward（群聊合并转发）。当前：{self._cfg('send_mode', 'text')}"
            )
            return
        self.config["send_mode"] = mode
        try:
            self.config.save_config()
            yield event.plain_result(f"已切换公布方式为：{mode}（已保存，下一轮生效）")
        except Exception as e:
            logger.warning(f"[天絮检测] 保存配置失败: {e}")
            yield event.plain_result(f"已切换公布方式为：{mode}（本次会话生效，保存失败：{e}）")
