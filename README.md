# moonjupyter 🥮

[![GitHub stars](https://img.shields.io/github/stars/Mr-Shi123/moonjupyter?style=social)](https://github.com/Mr-Shi123/moonjupyter)
[![CI](https://github.com/Mr-Shi123/moonjupyter/actions/workflows/ci.yml/badge.svg)](https://github.com/Mr-Shi123/moonjupyter/actions)

**A Jupyter kernel for MoonBit, with a pure-MoonBit ZeroMQ (ZMTP 3.1) protocol implementation.**

一个纯 MoonBit 实现的 Jupyter Kernel：在 JupyterLab / Jupyter Notebook / VS Code
Notebook 中交互式运行 MoonBit 代码。项目同时包含一个可独立复用的 ZMTP 3.1
协议库（greeting / 握手 / 帧编解码 / 会话状态机），填补了 MoonBit 生态在
消息协议基础设施上的空白。

> 生态调研结论（2026-09，mooncakes.io 全量关键词扫描 + GitHub/GitLink 检索）：
> MoonBit 生态已有 TLS、Raft、Kafka/NATS/AMQP 客户端、OpenTelemetry 等，
> 但 **ZeroMQ 与 Jupyter Kernel 均为空白**，本项目同时填补两者。

## Features

- **ZMTP 3.1**（纯 MoonBit，零 FFI）：greeting/NULL 安全机制/READY 握手、
  MORE/LONG/COMMAND 帧编解码、PING/PONG、可逐字节喂入的会话状态机、
  64 MiB 帧大小上限（协议级 DoS 防护）
- **Jupyter 协议 v5.4**：`<IDS|MSG>` 信封、HMAC-SHA256 签名（内置纯 MoonBit
  SHA-256/HMAC）、连接文件解析、UUID v4
- **五个通道**：shell/control/stdin（ROUTER）、iopub（PUB）、hb（REP 心跳回显）
- **cell 执行**：临时项目 + `moon run`，stdout/stderr 按行流式回传 iopub，
  成功/错误分别产出 execute_reply，metadata 携带真实执行耗时
- **交互体验**：TAB 补全（关键字 + 当前 cell 标识符）、`is_complete` 括号/字符串
  配对启发式（未闭合的 cell 不会误执行）、执行历史（最近 100 个 cell）、
  `inspect_request` 内置文档（Shift+Tab）
- **魔法命令**：`%help`、`%history`、`%stats`、`%time <code>`（报告执行耗时）、
  `%use <pkg>@<version>`（让 cell 引用 mooncakes 外部包）、`%reset`（清空状态）
- **健壮性**：cell 60s 超时强杀（杀进程树，覆盖死循环）、interrupt_request
  真正中断当前 cell、shutdown 回显 restart 标志并清理运行中的 cell、
  消息帧 16 MiB 上限、ZMTP 层 64 MiB 帧上限
- 跨平台：通过 Node.js 传输层运行（Windows/macOS/Linux）✨

## Quick Start

前置要求：`moon` CLI、Node.js >= 18、Jupyter（Lab/Notebook 均可）。
> Tip: 在 `moonup` 或 `moon` 官网装好 CLI 后，用 `moon --version` 验证。

```bash
git clone https://github.com/Mr-Shi123/moonjupyter
cd moonjupyter

# 校验 + 单元测试（需要 node）
moon check --target js
moon test  --target js

# 注册 kernelspec（写入 Jupyter kernels 目录）
node install.mjs
```

重启 JupyterLab，新建 Notebook 选择 **MoonBit** kernel，然后：

> 💡 在 JupyterLab 里可以直接 `Shift+Enter` 执行 cell，和 Python kernel 完全一致。

```moonbit
println("hello from MoonBit 🥮")
for i = 0; i < 5; i = i + 1 {
  println("cell line \{i}")
}
```

## Project Layout

```
moonjupyter/
├── bytesx/       字节工具（UTF-8 编解码、hex、拼接）
├── sha256/       纯 MoonBit SHA-256 + HMAC-SHA256（FIPS 180-4 / RFC 2104 向量测试）
├── zmtp/         ZMTP 3.1 协议核心（纯逻辑，可独立发布复用）
├── jupyter/      Jupyter 消息协议 v5.4（信封/签名/连接文件）
├── nodeff/       Node.js FFI：TCP server、zlib、子进程、随机数
├── kernel/       会话编排：通道绑定、请求分发、cell 执行
├── main/         入口（解析连接文件并启动 kernel）
├── launcher.mjs  Jupyter -> moon -> node 的启动器
├── install.mjs   注册 kernelspec
└── docs/ARCHITECTURE.md
```

## How it works

```
JupyterLab ──(5×TCP)── Node 传输层(nodeff) ── ZMTP 3.1 状态机 ── Jupyter 消息层 ── kernel 编排
                                                                    │
                                                          cell 写入临时项目
                                                          moon run src/main
                                                          stdout/stderr -> iopub
```

详见 [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)。

- **签名**：HMAC-SHA256 覆盖四个未压缩 JSON 部件（与 jupyter_client 一致），
  入站消息验证签名；出站暂不压缩，入站按 `0x78` 头自动检测 zlib 并解压。
- **ROUTER 路由**：回复时回传请求携带的 identity 帧前缀；hb 通道按 REP
  语义剥除/补回空帧后原样回显。

## Testing

```bash
moon test --target js   # SHA-256/HMAC 官方向量、ZMTP 握手与帧编解码、消息往返、UUID
```

覆盖：SHA-256 FIPS 180-4 向量、HMAC-RFC4231 用例、ZMTP greeting/READY/
长帧/逐字节喂入、Jupyter 消息签名往返与坏签名拒绝、连接文件解析。

## Limitations & Roadmap

当前为 MVP，已知限制与演进路线：

1. **跨 cell 状态（已支持，v0.1.9）**：内核按序重放全部 cell——以
   `let` / `fn` / `struct` 等声明开头的"定义 cell"拼接进顶层，后续 cell
   直接可用；语句 cell 只执行当前一次（历史副作用不重复）。含自写
   `fn main` 的 cell 作为完整程序单独执行，不进入重放。`%reset` 清空。
   → 路线：常驻会话进程 + 增量编译，避免重放编译时间随 notebook 增长。
2. **补全/检查为空实现**：`complete_request` / `inspect_request` 返回空。
   → 路线：对接 MoonBit 编译器诊断信息。
3. **stdin 未实现**、`interrupt_request` 忽略（interrupt_mode=message）。
4. ZMTP 仅 NULL 安全机制。→ 路线：基于 mooncrypt 的 CURVE（Curve25519）。
5. 传输层依赖 Node。→ 路线：`moonbitlang/async` 原生后端直连 TCP。

## Publishing to mooncakes.io

发布前把 `moon.mod` 里的 repository URL
改为你的 GitHub 用户名，然后 `moon publish`。`zmtp` 包可独立作为
`<user>/moonjupyter/zmtp` 被其他项目引用。

## Changelog

- **v0.2.0** — 富文本输出与错误诊断结构化（申报书规划第三项）：
  `%html` / `%md` 魔法经 display_data 输出 MIME bundle（Jupyter 渲染
  HTML/Markdown）；编译错误 traceback 结构化为 `main.mbt:行:列: 消息`
  条目（剥离 box-drawing 框线与临时路径噪音）。asyncff 原生传输
  （规划第二项）里程碑已就位：`asyncff/` 子项目验证 moonbitlang/async
  的 TCP 原语在 Windows IOCP 上可用，kernel 迁移待上游 MinGW 兼容修复
- **v0.1.9** — 累计重放（申报书规划第一项）：跨 cell 状态累积。定义 cell
  （以 let/fn/struct 等声明开头）拼接进 notebook 顶层，后续 cell 直接可用；
  语句 cell 只执行当前一次，历史副作用不重复；自写 `fn main` 的 cell 单独
  执行；`%reset` 清空重放状态
- **v0.1.8** — magic commands `%timeout` (view/set the per-cell wall-time
  limit) and multi-package `%use a@v b@v`; cell output fragment is capped
  at 1 MiB so newline-less floods cannot grow memory unbounded; registry
  failures in cells now carry a `%use` hint; cells launched right after a
  force-kill wait ~1s so the OS can tear the killed tree down (an instant
  moon run used to fail with exit 1 and no output)
- **v0.1.7** — magic commands `%time` / `%use <pkg>@<ver>` / `%reset`;
  `%use` lets cells import mooncakes packages (generates current moon.mod /
  moon.pkg configs, replacing the deprecated JSON files); is_complete now
  treats an unterminated string literal as incomplete
- **v0.1.6** — ZMTP 层帧大小上限与会话关闭短路（协议级 DoS 加固），execute
  尊重 store_history，execute_reply metadata 携带真实耗时，断连清理 peer
  计数，README 功能文档更新
- **v0.1.5** — fix Date.now() Int32 overflow in nodeff FFI, recurring
  periodic stats/maintenance timers (`every()`), real per-cell execution
  timing and failure counting in metrics, kernel_info reports the actual
  release version
- **v0.1.4** — kernel observability (structured logging, per-peer counters,
  execution metrics, periodic stats), 16 MiB message-part size limit (DoS
  guard), `nodeff.now_seconds()`, restore jupyter inline tests
- **v0.1.3** — bind_channel doc, UTF-8 surrogate comment, bump version
- **v0.1.2** — CI tweaks, kernel pkg import sort, security doc section
- **v0.1.1** — bump version, README badges, ZMTP flag annotations

## License

MIT — see [LICENSE](LICENSE).
