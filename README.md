# 天絮API可用模型检测（AstrBot 插件）

AstrBot v4（Star API）插件：**每 30 分钟（北京时间 :00/:30，自 00:00 起）** 在机器人所在服务器上检测天絮API全部模型的连通性与**首字延迟**，检测**完全结束后**将可用模型按延迟升序排名（延迟最小的为 No.1），公布到**群白名单**。

## 功能

- **全量模型检测**：运行时从 `/v1/models` ∪ `/api/pricing` 自动合并去重，新模型上线即被纳入，永不遗漏。
- **限速**：全局限速默认 10 次/分钟（串行槽位分发，任意 60s 窗口内启动数 ≤ rpm），默认并发 10。一轮约 13-16 分钟，落在 30 分钟槽位内。
- **首字延迟**：流式测量到首个 content / reasoning / tool_calls / finish_reason（聊天）、data（embedding）、results（rerank）为止。
- **判定**：首字延迟 ≤ 可用阈值（默认 60000ms）为**可用**，只公布可用模型；≥300000ms 或出错为不可用（Cloudflare 524 在 ~126s 掐断会自动识别）。
- **公布形式**（管理员可选）：
  - `text`：普通文本消息（所有平台通用，超长自动分片）
  - `forward`：群聊合并转发卡片，每个模型一个节点（仅 OneBot v11 / aiocqhttp；其它平台自动降级为文本）
- **群白名单**：只在配置的 QQ 群号列表里公布。
- **手动触发**：白名单群成员或管理员在群里发 **「状态」**（不用斜杠、不用 @，直接两个字即可）立即触发一轮检测；管理员也可用 `/mc_run`。检测完成后结果自动公布到白名单群（私聊/非白名单群触发时，结果会回发给你）。
- 命令：`/mc_status`（查看上次结果）、`/mc_mode text|forward`（切换公布方式，管理员）。

## 安装

1. 把整个 `astrbot_plugin_tianxu_checker` 文件夹放进 AstrBot 的插件目录（一般 `data/plugins/` 下，支持直接在 WebUI 插件管理中安装）。
2. 重启 AstrBot 或重载插件。
3. 在插件配置里填写：
   - `api_key`：天絮API的 API Key（sk- 开头）
   - `group_whitelist`：要公布结果的 QQ 群号列表
   - （可选）`send_mode`：`text` 或 `forward`；`base_url`、`rpm` 等按需调整

## 公布格式（示例）

```
天絮API目前可用模型
No.1 BAAI/bge-reranker-v2-m3
717ms
No.2 deepseek-v4-flash
2398ms
No.3 glm-5.2
4124ms
...
（共 15 个可用 · 北京时间 2026-01-01 00:16 · 检测用时 14m32s）
```

## 配置项

| 键 | 类型 | 默认 | 说明 |
| --- | --- | --- | --- |
| base_url | string | https://chat-api4.087654.xyz | API 地址 |
| api_key | string | 空 | API Key（sk- 开头） |
| rpm | int | 10 | 全局限速（次/分钟） |
| max_concurrency | int | 10 | 最大并发 |
| timeout_ms | int | 1000000 | 单请求超时（毫秒） |
| green_ms | int | 60000 | 可用延迟阈值（毫秒），首字 ≤ 此值才公布 |
| include_yellow | bool | false | 是否连“一般”（≤300000ms）模型也列入 |
| prompt | text | 你好，请回复一个字符。 | 检测用提示词 |
| strict | bool | true | 严格模式：模型报错即不可用 |
| group_whitelist | list | [] | 公布群白名单（QQ 群号） |
| send_mode | string | text | text / forward |
| announce_header | string | 天絮API目前可用模型 | 消息标题 |
| max_models_per_broadcast | int | 50 | forward 模式最大转发节点数 |
| bot_display_name | string | 天絮API检测 | 转发节点昵称 |
| bot_uin | string | 空 | 转发节点 QQ 号 |
| timezone | string | Asia/Shanghai | 调度时区 |

## 注意

- 定时任务由插件内部 apscheduler 承担（AstrBot 自带该依赖），时区默认北京时间，:00/:30 触发，检测完才发群。
- 上一轮未跑完时，下一轮触发会被跳过（不会叠加）。
- 本插件会真实调用 API（每次全量检测约 131 次请求），请勿在同一机器人上重复安装多个副本。
- 更多命令可在机器人里输入 `/mc_status` 查看。
