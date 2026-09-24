# 架构 / Architecture

## 分层

```
+--------------------------------------------------------------+
|  JupyterLab / Jupyter Notebook (客户端: REQ / DEALER / SUB)   |
+------------------+-------------------------------------------+
                   | TCP (5 channels)
+------------------v-------------------------------------------+
| nodeff  Node.js 传输层 (net / zlib / child_process / crypto) |
+--------------------------------------------------------------+
| zmtp   ZMTP 3.1 协议核心（纯 MoonBit，无 FFI）               |
|   - 64 字节 greeting / NULL 安全机制 / READY 命令            |
|   - 帧编解码 (MORE / LONG / COMMAND 标志)                    |
|   - 会话状态机: feed(bytes) -> [Event]                       |
+--------------------------------------------------------------+
| jupyter Jupyter 消息协议 (纯 MoonBit)                        |
|   - <IDS|MSG> 信封 / HMAC-SHA256 签名 (sha256 包)            |
|   - 连接文件解析 / UUID v4 / JSON 头部                       |
+--------------------------------------------------------------+
| kernel  会话编排: 5 通道绑定、请求分发、cell 执行            |
+--------------------------------------------------------------+
```

> **纯逻辑层 vs FFI 层**：`zmtp` 和 `jupyter` 两层完全不依赖 FFI，可以在任何
> 宿主（Node.js、原生 TCP、测试桩）上跑相同的单元测试。
>
## 五个通道

| 通道 | Kernel 端 socket | 用途 |
|------|------------------|------|
| shell | ROUTER | execute_request / kernel_info 等 |
| control | ROUTER | 控制面请求（shutdown、interrupt） |
| stdin | ROUTER | 用户输入（当前为桩） |
| iopub | PUB | status / stream / execute_input 广播 |
| hb | REP | 心跳，原样回显 |

## ZMTP 会话状态机

`zmtp` 包完全纯函数化：宿主把 TCP 收到的字节喂给 `ZmtpSession::feed`，
得到事件列表（PeerGreeting / PeerReady / Message / PingReceived / Error）；
发送方向由 `greeting_bytes` / `ready_command` / `encode_message` 等纯函数
产生线缆字节。这使得整个协议栈可以在任何后端上做单元测试。

## 签名与压缩

- 签名对 header/parent/metadata/content 四个 **未压缩** JSON 字节串计算
  HMAC-SHA256（与 jupyter_client 一致），kernel 侧验证入站签名。
- 入站部件按 zlib 头字节 `0x78` 自动检测并解压；出站部件暂不压缩
  （jupyter_client 对两种都能解析）。
- ROUTER 回复时原样回传请求携带的 identity 帧前缀（REQ/DEALER 客户端
  都能正确处理），hb 通道按 REP 语义剥掉/补上空帧后回显。

> ROUTER 侧 identity 帧处理是 kernel 对接 REQ/DEALER 客户端的关键，
> 漏传或乱序都会导致客户端挂起等待。

## cell 执行模型

每个 cell 写入临时 MoonBit 项目（`moon.mod.json` + `src/main`），若 cell
代码不含 `fn main` 则自动包裹生成入口，然后 `moon run src/main` 执行，
stdout/stderr 按行以 `stream` 消息推送到 iopub，退出码决定 execute_reply
的 `status`（ok/error）。这是无状态执行（跨 cell 不共享定义）——见 README 路线图。
