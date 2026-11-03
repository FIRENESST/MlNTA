# 标准 HTTP/JSON 网关

应用只与 HTTP 接口交互。`mintagw proxy` 和 `mintagw expose` 之间使用标准 gRPC/HTTP2，不发送 BEEP 帧。两侧可以独立部署，也可以使用 `core/bridge/mintagw.example.toml` 在一个进程运行。

## 全语言接入

任何能发送 HTTP 的语言都具备相同能力，无需维护另一份协议栈。Rust、Python、Node、Java/JVM、.NET、Go、C/C++、Fortran、Ruby、PHP、Zig、LuaJIT、Swift、Nim、Dart、Julia、Pascal、D、COBOL、Shell、PowerShell、Perl、Tcl、R、Elixir、Erlang、Haskell、Crystal、Lua、OCaml 等均可使用其 HTTP 库，或者调用 curl。是否有该语言的 gRPC/QUIC 库不影响接入。

`examples/http` 包含六种语言的无 MINTA 依赖客户端，也可从 OpenAPI 生成其它语言的 HTTP 客户端。旧 `native`/`bindings` 继续连接 BEEP。旧 `sdk` 的普通 HTTP 成功请求可复用，但 BEEP 专有错误码、SASL、管理接口不属于新 API；新应用使用标准 HTTP 状态与 JSON 错误。

## 普通 HTTP

原来的请求直接发往网关同一路径，方法、查询、应用头、二进制体和后端状态保留。gRPC 应用客户端使用网关的 h2c 入口，后端配置 `h2c://host:port`。支持流式请求/响应、全双工及两侧 trailer；WebSocket 使用 HTTP/1.1 Upgrade。

逐跳头以及 `Connection` 指定的头不跨网关；`TE: trailers` 保留。每个内部 gRPC data 包最多 64 KiB，单包解码上限 128 KiB；大文件通过多个包流式发送。HTTP/2 扩展 CONNECT 不在当前实现中。

路由采用最长前缀匹配，按整段路径匹配：前缀 `/api` 匹配 `/api`、`/api/x`、`/api?q`，不匹配 `/apiary`。可选 `X-Minta-Upstream` 选择配置中的上游，`X-Minta-Timeout-Ms` 指定更短截止时间。迁移旧客户端时接受同义的 `X-Beep-Upstream`、`X-Beep-Timeout-Ms`；这些控制头不交给后端。任何能访问入口的客户端都能用 `X-Minta-Upstream` 选中任一已配置上游，路由不是访问控制。

含 `.` 或 `..` 路径段（包括 `%2e` 编码形式）的目标返回 400，避免越出后端基础路径（如 `https://host/base`）；`..hidden` 这类普通段名不受影响。

网关自身占用 `/healthz`、`/readyz`、`/openapi.json`、`/v1/call` 和 `/__beep/health`，后端同名路径无法经普通 HTTP 透传访问。

## `POST /v1/call`

必须使用 `Content-Type: application/json`，例如：

```json
{"method":"POST","target":"/orders?dry_run=1","headers":{"X-Request-Id":"order-7"},"json":{"id":7},"timeout_ms":5000}
```

`method` 默认为 GET；`target` 是必填的 origin-form 路径，不接受完整 URL 或 `//host/path`。目的主机只能来自网关配置。请求头可以是字符串值对象，或 `[["name","value"], ...]` 数组以保留重复头。无效字段/方法/头、多个 body 表示和无效 base64 返回 HTTP 400。

| 请求字段（三选一） | 含义 |
|---|---|
| `body` | UTF-8 原文字节 |
| `body_base64` | 标准 base64 编码的任意二进制字节 |
| `json` | 任意 JSON 值，包含 null；未指定 Content-Type 时自动设 application/json |

传输成功返回 HTTP 200，例如：

```json
{"status":404,"headers":[["content-type","text/plain"]],"trailers":[],"body":"not found"}
```

`status` 是后端 HTTP 状态，包含后端 404/500。UTF-8 应答提供 `body`，否则提供 `body_base64`。头和 trailer 为数组。JSON API 会缓冲，不用于无限流或升级；这些场景使用普通 HTTP 路径。

网关失败返回标准 HTTP 错误状态与：

```json
{"error":{"code":"deadline_exceeded","message":"request deadline exceeded"}}
```

413 表示请求/头超过限额；502 表示上游应答无效、不完整或 JSON 缓冲应答超过限制；503 表示对端/后端不可达；504 表示截止时间到期。流式 HTTP 应答已经开始时，通过中断 body 表示失败，不能再次更改状态。

## 限额、超时与取消

应答可能先于上传结束（提前拒绝、超限、出错）。这时入口继续读取并丢弃剩余上传，最多 5 秒、最多再读 `max_body_bytes` 字节，客户端能读到应答，不会因连接被重置而丢失。

两侧 `max_body_bytes` 分别限制请求体累计字节数，默认 64 MiB。已知长度、chunked、JSON/base64 均执行限制，JSON 另有编码层大小保护。大上传需要同时调高两侧配置；升级后的 WebSocket 隧道不受上传大小限制。

`max_response_bytes` 只限制 `/v1/call` 缓冲应答，默认 64 MiB。普通 HTTP 响应按块转发。两侧 `max_in_flight` 默认 64；额度随调用/应答体生命周期释放，排队受调用截止时间约束。

网关间所有调用共用一条 HTTP/2 连接。网关拥有的每一跳（h2c 入口、网关间 gRPC、h2c/h2 后端）每个流的接收窗口为 1 MiB，连接窗口覆盖全部并发流的窗口之和：读得慢或不读的客户端/后端只占住自己那个流，不会冻结同一连接上的其它调用。最坏情况下每条连接缓冲 `max_in_flight` × 1 MiB。

`timeout_ms` 默认 60000（60 秒），覆盖调用及其流的总时长；调用者只能指定更短时间，后端网关也有自身上限。长连接须调高两侧配置。断开连接、丢弃 body、超时会取消对应 gRPC 流并停止后端 I/O。内部协议要求显式 End，缺失 End 视为中止，不能当作成功的短请求/应答。

## TLS 与内部认证

内部 gRPC 支持标准 TLS/服务端证书验证与可选 Bearer token，应用不需要处理内部认证。token 由 `--token-env` 指定环境变量读取。例如 Shell：

```sh
export MINTA_INTERNAL_TOKEN='your-secret'
mintagw expose --listen 0.0.0.0:10290 --backend http://127.0.0.1:3000 \
  --cert-file server.pem --key-file server-key.pem --token-env MINTA_INTERNAL_TOKEN
mintagw proxy --upstream https://bridge.internal:10290 --ca-file ca.pem \
  --token-env MINTA_INTERNAL_TOKEN
```

支持 HTTPS 后端与 `--backend-ca-file` 私有 CA。入口默认仅监听 loopback；对外 HTTPS、用户级认证或限流可用标准 HTTP 反向代理。`/healthz` 检查进程，`/readyz` 验证配置的 gRPC 对端连接与 token，不探测应用健康路径。

## 验证与兼容

`python tests/standard_test.py --strict` 使用真实后端和独立网关进程，验证 JSON/二进制/trailer、限额、并发、路由、超时和无限流取消、TLS/token、真实 gRPC/WebSocket 及可用语言的标准 HTTP 客户端。可设置 `MINTAGW` 指向待测二进制；缺失语言工具链明确报告 SKIP。

`tests/run_all.py` 保留原多语言 BEEP 矩阵，`--profile debug` 用于开发，默认 release。修复了 Go 模块文件被忽略、rpc:1 回退未使用监听器缓冲上限、以及无限 drain 占用通道池。BEEP drain 超时中止整个会话，新 gRPC 按流取消。BEEP 规范继续见 `PROTOCOL.md`。
