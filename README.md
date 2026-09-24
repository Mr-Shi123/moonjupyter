# moonjupyter 🥮

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
  MORE/LONG/COMMAND 帧编解码、PING/PONG、可逐字节喂入的会话状态机
- **Jupyter 协议 v5.4**：`<IDS|MSG>` 信封、HMAC-SHA256 签名（内置纯 MoonBit
  SHA-256/HMAC）、连接文件解析、UUID v4
- **五个通道**：shell/control/stdin（ROUTER）、iopub（PUB）、hb（REP 心跳回显）
- **cell 执行**：临时项目 + `moon run`，stdout/stderr 按行流式回传 iopub，
  成功/错误分别产出 execute_reply
- 跨平台：通过 Node.js 传输层运行（Windows/macOS/Linux）

## Quick Start

前置要求：`moon` CLI、Node.js >= 18、Jupyter（Lab/Notebook 均可）。

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

1. **无状态 cell**：每个 cell 独立 `moon run`，跨 cell 不共享定义。
   → 路线：常驻会话进程 + 增量编译/求值。
2. **补全/检查为空实现**：`complete_request` / `inspect_request` 返回空。
   → 路线：对接 MoonBit 编译器诊断信息。
3. **stdin 未实现**、`interrupt_request` 忽略（interrupt_mode=message）。
4. ZMTP 仅 NULL 安全机制。→ 路线：基于 mooncrypt 的 CURVE（Curve25519）。
5. 传输层依赖 Node。→ 路线：`moonbitlang/async` 原生后端直连 TCP。

## Publishing to mooncakes.io

发布前把 `moon.mod.json` 里的 `moonjupyter/moonjupyter` 与 repository URL
改为你的 GitHub 用户名，然后 `moon publish`。`zmtp` 包可独立作为
`<user>/moonjupyter/zmtp` 被其他项目引用。

## License

MIT — see [LICENSE](LICENSE).
