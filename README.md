# MINTA HTTP/JSON Gateway

**应用统一使用标准 HTTP/JSON，网关之间使用流式 gRPC。任何能发 HTTP 的语言都能接入，无需实现专用协议、安装 MINTA SDK 或生成 Protobuf 客户端。**

```text
任意语言客户端 → HTTP/JSON → mintagw proxy
                                ⇅ gRPC / HTTP/2，可选 TLS + Bearer
任意语言后端 ← HTTP/HTTPS/h2c ← mintagw expose
```

## 推荐入口

需要 Rust 1.88+，Protobuf 编译器随构建依赖提供，无需另装 protoc。

```sh
cd core
cargo build --release --locked
# 后端侧：现有 HTTP 服务无需修改
mintagw expose --listen 127.0.0.1:10290 --backend http://127.0.0.1:3000
# 应用侧
mintagw proxy --listen 127.0.0.1:8080 --upstream http://127.0.0.1:10290
```

原来的 HTTP 请求直接发往网关同一路径。也可以使用统一 JSON 接口：

```sh
curl -H 'Content-Type: application/json' \
  --data '{"method":"POST","target":"/echo","json":{"hello":"world"}}' \
  http://127.0.0.1:8080/v1/call
```

应答示例（外层 HTTP 200 表示调用完成，`status` 是后端状态，包含后端 404/500）：

```json
{"status":200,"headers":[["content-type","application/json"]],"trailers":[],"body":"{\"hello\":\"world\"}"}
```

请求体三选一：`json`（包含 null）、`body`（UTF-8 文本）、`body_base64`（二进制）。应答使用 `body` 或 `body_base64`，头/trailer 用数组保留同名字段。网关失败返回标准 HTTP 错误状态与 `{"error":{"code":"...","message":"..."}}`。

- [HTTP API 与运行说明](docs/HTTP_GATEWAY.md) · [OpenAPI 3.1](docs/HTTP_API.openapi.json)（运行时 `/openapi.json`）
- [网关间 Protobuf 契约](core/bridge/proto/bridge.proto) · [TOML 配置](core/bridge/mintagw.example.toml)
- [无 MINTA 依赖的客户端](examples/http/)：Python、Node.js、Java、Go、C#、PowerShell；其它语言使用自身 HTTP 库或 curl，语言覆盖不依赖原生 gRPC 库

普通 HTTP 路径支持流式上传/响应、trailer、gRPC 四种调用模式和 HTTP/1.1 WebSocket Upgrade。`/v1/call` 是有大小上限的缓冲 JSON API。请求体限制在两侧各自生效，默认 64 MiB；大上传需调高两侧 `--max-body-bytes`。JSON 响应另有 `--max-response-bytes` 限制。

`--timeout-ms` 默认 30000，是调用及其流的总时长；长连接应调高两侧配置。断开连接/丢弃应答会取消相应 gRPC 流，释放并发额度并终止后端 I/O。HTTP 入口支持 HTTP/1.1、h2c，入口 HTTPS 可由标准反向代理终结；内部 gRPC 支持 TLS/证书验证和可选 Bearer 认证。

```sh
cargo test --manifest-path core/Cargo.toml --workspace --locked
cargo build --manifest-path core/Cargo.toml --workspace --locked
python -m pip install -r tests/requirements.txt
python tests/standard_test.py --strict
# 历史 BEEP/FFI/SDK 兼容矩阵，缺失工具链明确标为 SKIP
python tests/run_all.py --build
```

标准链路测试覆盖 HTTP/JSON、二进制/trailer、限额、超时和无限流取消、并发、路由、真实 gRPC/WebSocket、TLS/认证及可用语言的标准 HTTP 客户端。历史的 71 项结果对应下方 BEEP 版本，不代表新链路的实测结果。

---

# BEEP 兼容接口

以下 `beepgw`、`native/`、`bindings/` 和 `sdk/` 接口保留供现有应用使用；新集成优先使用上面的 HTTP/JSON 入口。

把**几乎任何编程语言**的后端请求对接到 **RFC 3080/3081 BEEP** 传输层的适配器套件。

```
           出向（应用 → BEEP）                                   入向（BEEP → 应用）
┌───────────────────────────┐                              ┌───────────────────────────┐
│ 任意语言 HTTP 客户端       │──HTTP/1.1, h2c──┐           │ 任意语言 HTTP 服务         │
│ COBOL / 嵌入式 / shell+nc │──TCP JSON-Lines─┤           │ Express/Spring/Django/... │
└───────────────────────────┘                 ▼           └─────────────▲─────────────┘
                                        ┌──────────┐                     │ HTTP
                                        │  beepgw  │◄────── BEEP ───────►│ beepgw expose
                                        └────┬─────┘                     │
                                             │ BEEP (TCP, 可选 TLS)
┌───────────────────────────┐                ▼
│ 原生库：Rust Python Node   │◄──────── BEEP 网络 / 你的 BEEP 服务 ────────►
│ Java .NET Go              │
│ C ABI：C C++ Fortran Ruby │
│ PHP Zig Swift Dart ...    │
└───────────────────────────┘
```

- **BEEP 层**：完整实现帧格式、SEQ 滑动窗口流控、通道管理（start/close/greeting）、MSG/RPY/ERR/ANS/NUL、TLS 调谐 profile、SASL PLAIN 认证。
- **RPC profile `urn:minta:beep:rpc:1`**：在 BEEP 通道上承载 HTTP 语义请求（方法、目标、头、二进制体、状态码、流式应答）。
- **RPC profile `urn:minta:beep:rpc:2`**（rpc:1 的超集，自动协商、自动回落）：流式请求体（不限大小）、全双工、请求/应答 trailer、协议升级——因此 **gRPC**（四种调用模式）和 **WebSocket** 都能经网关穿过 BEEP。
- 规范：[docs/PROTOCOL.md](docs/PROTOCOL.md) · 语言覆盖矩阵：[docs/LANGUAGES.md](docs/LANGUAGES.md)

## 三种接入方式

| 方式 | 覆盖 | 说明 |
|---|---|---|
| **原生库** | Rust、Python、Node.js/TS、Java（及 Kotlin/Scala/Groovy/Clojure）、C#/F#/VB.NET、Go | 纯本语言实现，无外部依赖，客户端 + 服务端 + TLS |
| **C ABI `libbeep`** | C、C++、Fortran、Ruby、PHP、Zig、LuaJIT、Swift、Nim、Dart、Julia、Free Pascal、D、COBOL | Rust 核心导出的 C 接口，原生 BEEP 性能 |
| **网关 `beepgw`** | **任何能发 HTTP 的语言**（零代码）；另有 TCP JSON-Lines | 加 SDK：Shell、PowerShell、Perl、Tcl、PHP、Ruby、R、Elixir、Erlang、Haskell、Crystal、Lua、OCaml |

以上全部有测试并实际跑通，平台见 [docs/LANGUAGES.md](docs/LANGUAGES.md)。

入向：`beepgw expose` 让**任何语言的现成 HTTP 服务**零改动挂到 BEEP；Python（ASGI/WSGI）、Node（Express 等）、Go（`net/http`）还能在进程内直接服务 BEEP。

## 快速开始

```bash
cd core
cargo build --release          # 产物：target/release/beepgw(.exe)、beep.dll / libbeep.so
```

```bash
# 1) 一个 BEEP RPC 测试服务端（/echo /stream/N /status/N /err/N /sleep/MS /bytes/N /inspect）
beepgw echo --listen 127.0.0.1:10288

# 2) 出向网关：任何语言把 HTTP 发到 8080，网关转成 BEEP
beepgw proxy --listen 127.0.0.1:8080 --upstream 127.0.0.1:10288 --jsonl 127.0.0.1:8081
curl -X POST -H 'Content-Type: text/plain' --data 'hello' http://127.0.0.1:8080/echo

# 3) 入向：把已有 HTTP 服务（任意语言）挂到 BEEP
beepgw expose --listen 0.0.0.0:10289 --backend http://127.0.0.1:3000

# 4) 命令行直接发 BEEP 请求（类似 curl）
beepgw call --upstream 127.0.0.1:10288 -i POST /echo -d 'hi'
```

gRPC 与 WebSocket（任何语言的 gRPC / WebSocket 库，零改动）：

```bash
# BEEP 另一侧：把 gRPC 服务（h2c）和 WebSocket 服务挂到 BEEP
beepgw expose --listen 0.0.0.0:10290 --backend h2c://127.0.0.1:50051
beepgw expose --listen 0.0.0.0:10291 --backend http://127.0.0.1:9000
# 应用侧：gRPC 客户端（明文 h2c）连 8090，WebSocket 客户端连 ws://127.0.0.1:8091/...
beepgw proxy --listen 127.0.0.1:8090 --upstream 10.0.0.5:10290
beepgw proxy --listen 127.0.0.1:8091 --upstream 10.0.0.5:10291
```

认证（SASL PLAIN，建议配合 TLS；身份以 `X-Beep-User` 交给后端）：

```bash
printf 'alice:sha256:%s\n' "$(printf %s 's3cret' | sha256sum | cut -d' ' -f1)" > users.txt
beepgw expose --listen 0.0.0.0:10289 --backend https://api.internal --sasl-users users.txt --tls-cert c.pem --tls-key k.pem
beepgw proxy --upstream gw.internal:10289 --tls-server-name gw.internal --sasl-user alice   # 口令取自 $BEEPGW_SASL_PASSWORD
```

完整配置（多上游、路由、TLS、JSON-Lines、入向）：[core/gateway/beepgw.example.toml](core/gateway/beepgw.example.toml)，`beepgw run -c beepgw.toml`。

## 各语言速览

<details><summary>Python（原生，asyncio / 同步 / ASGI / WSGI）</summary>

```python
from minta_beep import RpcClient, RpcServer, ServerResponse
async with RpcClient("127.0.0.1", 10288) as c:
    r = await c.call("POST", "/orders", json={"id": 7})

from minta_beep.sync import Client            # 阻塞版
Client("127.0.0.1", 10288).get("/health")

from minta_beep.adapters import asgi_handler  # FastAPI / Starlette 直接服务 BEEP
await (await RpcServer(asgi_handler(app)).start("0.0.0.0", 10288)).serve_forever()
```
</details>

<details><summary>Node.js / TypeScript（原生，含 Express 适配）</summary>

```js
const { RpcClient, RpcServer, httpHandler } = require('@minta/beep');
const r = await new RpcClient('127.0.0.1', 10288).call('POST', '/orders', { json: { id: 7 } });
await new RpcServer(httpHandler(expressApp)).listen(10288, '0.0.0.0');
```
</details>

<details><summary>Java（原生，Java 11+）</summary>

```java
try (RpcClient c = new RpcClient("127.0.0.1", 10288)) {
    Rpc.Response r = c.call("POST", "/orders", Headers.of("Content-Type", "application/json"), body);
}
new RpcServer(req -> Rpc.Reply.of(200, null, "hi")).start("0.0.0.0", 10288);
```
</details>

<details><summary>C# / F# / VB.NET（原生，.NET 8+）</summary>

```csharp
await using var c = new RpcClient("127.0.0.1", 10288);
var r = await c.CallAsync("POST", "/orders", new Headers { { "Content-Type", "application/json" } }, body);
var s = new RpcServer(req => Task.FromResult(RpcReply.Of(200, null, "hi"))).Start("0.0.0.0", 10288);
```
</details>

<details><summary>Go（原生，含 net/http 适配和 RoundTripper）</summary>

```go
c := beep.NewClient("127.0.0.1:10288", nil)
r, err := c.Call(ctx, "POST", "/orders", beep.Headers{{"Content-Type", "application/json"}}, body)

srv, _ := beep.NewServer(":10288", beep.HTTPHandler(ginEngine), nil, nil)   // 任何 http.Handler
hc := &http.Client{Transport: beep.NewTransport(c)}                         // 任何 http.Client 走 BEEP
```
</details>

<details><summary>Rust</summary>

```rust
let c = RpcClient::new("127.0.0.1:10288", ClientOptions::default());
let r = c.call(RpcRequest::new("POST", "/orders").body("{}")).await?;
```
</details>

<details><summary>C / C++ / Fortran / Ruby / PHP / Zig（libbeep）</summary>

```c
beep_client *c = beep_client_new("127.0.0.1:10288", 1, 64, 30000);
beep_response *r;
beep_call(c, "POST", "/orders", "Content-Type: application/json", body, len, &r);
printf("%d %s\n", beep_response_status(r), beep_response_body(r, NULL));
```
```cpp
beep::Client c("127.0.0.1:10288");  auto r = c.call("GET", "/health");
beep::Server s("0.0.0.0:10288", [](const beep::Request& q, beep::Reply& out) { out.body = "hi"; });
```
```ruby
Beep::Client.new('127.0.0.1:10288').call('POST', '/orders', body: '{"id":7}')
```
```php
(new Minta\Beep\Client('127.0.0.1:10288'))->call('POST', '/orders', [], '{"id":7}');
```
</details>

<details><summary>任何其它语言（经网关）</summary>

直接用该语言的 HTTP 客户端请求 `http://网关:8080/<任意路径>`。网关控制头：`X-Beep-Upstream`（选上游）、`X-Beep-Timeout-Ms`。
BEEP 层失败时返回映射后的 HTTP 状态 + `X-Beep-Error-Code` + `{"error":{"code","message"}}`。

没有 HTTP 库？用 TCP JSON-Lines（8081）：
```
{"id":1,"method":"POST","target":"/orders","headers":{"Content-Type":"application/json"},"body":"{\"id\":7}"}
```
</details>

## 测试

```bash
python tests/run_all.py --build
```

一条命令构建并运行全部测试：Rust 核心单元/集成测试（含 rpc:2 全双工、trailer、中止、80 MiB 流式上传、回落 rpc:1、SASL）、6 个原生库各自的全套用例（与 Rust 服务端互通 + 本语言自通 + 极小窗口大消息 + 流水线 + ANS 流 + 中止 + ERR + 超时回收 + TLS 双向）、14 个 FFI 绑定、13 个网关 SDK、4 种 JVM 语言、JSON-Lines、入向链路（含 HTTPS 后端）、**真实 gRPC（grpcio ↔ grpcio：unary/服务端流/客户端流/交替 bidi/错误码/超时）**、**真实 WebSocket**、SASL，以及 **4 种客户端 × 6 种服务端** 的跨语言矩阵。找不到工具链的语言自动跳过。

最近一次结果：

| 平台 | 结果 | 工具链 |
|---|---|---|
| Ubuntu 24.04（WSL2） | **71 通过，0 失败，0 跳过** | Rust 1.99 / Python 3.12 / Node 18 / JDK 21 / .NET 8 / Go 1.22 / PHP 8.3 / Ruby 3.2 / Zig 0.17 / GCC 13，以及 LuaJIT、Swift 6.4、Nim、Dart 3.13、Julia 1.13、FPC、LDC、GnuCOBOL、R、Elixir、Erlang、GHC、Crystal、Lua、OCaml、Kotlin、Scala 3、Groovy、Clojure |
| Windows 11 | **71 通过，0 失败，0 跳过** | Rust 1.98 / Python 3.13 / Node 24 / JDK 21 / .NET 10 / Go 1.27 / PHP 8.5 / Ruby 4.0 / Zig 0.17 / GCC 16，以及 LuaJIT、Swift 6.4、Nim 2.4、Dart 3.13、Julia 1.13、FPC 3.2、LDC 1.43、GnuCOBOL 3.2、R 4.6、Elixir 1.20、Erlang/OTP 29、GHC 9.10、Crystal 1.22、Lua 5.4、OCaml 5.4、Kotlin、Scala 3、Groovy、Clojure |

Windows 上补齐工具链时的几个坑（`tests/run_all.py` 已处理或注明）：

- **Free Pascal**：Windows 的 `fpc` 驱动默认出 32 位程序，加载不了 64 位 `beep.dll`，需 `-Px86_64`。
- **Swift**：Rust 在同目录同时产出静态库 `beep.lib` 和导入库 `beep.dll.lib`，要让 Swift 链导入库；若 `swift` 启动即崩（`0xC0000142`），设 `MIMALLOC_DISABLE_REDIRECT=1`，此时测试改用 `--build-system native`（默认构建系统不把该变量传给 `swift-frontend`）。
- **GnuCOBOL**：3.x 生成 K&R 风格 C 原型，GCC 15+ 默认 C23 会拒绝，需 `-A -std=gnu17`；测试以 `cobc -m` + `cobcrun` 运行，避免 PATH 上其它 MinGW 的同名 DLL 抢先加载。
- **Haskell**：用 cabal 包环境时，除 `http-client`、`http-types` 外还要暴露 `bytestring`。

## 目录

```
core/beep        Rust：BEEP 协议 + RPC profile（库）
core/gateway     Rust：beepgw 网关（HTTP/h2c/JSON-Lines ⇄ BEEP，入向、echo、call）
core/ffi         Rust：libbeep C ABI + include/beep.h
native/          原生实现：python  node  java  dotnet  go
bindings/        C ABI 绑定：c cpp fortran ruby php zig luajit swift nim dart julia pascal d cobol
sdk/             网关 SDK：shell powershell perl tcl php ruby r elixir erlang haskell crystal lua ocaml
examples/        fsharp vbnet jvm(kotlin scala clojure groovy)
docs/            PROTOCOL.md（规范）LANGUAGES.md（覆盖矩阵）
tests/           run_all.py（总测试）backend.py clients/ grpc_test.py ws_test.py stream_test.py
```

## 已知限制

- **SASL** 只实现了 PLAIN（RFC 4616），没有 SASL 安全层；口令以明文传输，需配合 TLS。SCRAM、EXTERNAL（TLS 客户端证书）未实现。
- **rpc:2**（流式请求、全双工、trailer、升级）由 Rust 核心和 BEEP 网关完整提供。C ABI 客户端能协商 rpc:2，但导出 API 仍是缓冲请求/响应，不提供全双工接口。Python/Node/Java/.NET/Go 原生库实现的是 rpc:1，可以自动回落互通。新 HTTP/gRPC 网关不依赖这些原生库。
- BEEP 客户端对超时/放弃的响应最多后台读取 `ClientOptions::drain_timeout`（默认 5 秒），仍不结束则中止该会话并重建。BEEP 无法单独取消繁忙通道，会影响同会话其它请求；新 gRPC 链路按流取消。
- **TLS**：采用标准 TLS 调谐 profile，在会话开始时协商；监听端配置 TLS 后要求所有会话先走 TLS。
- WebSocket 只支持 HTTP/1.1 `Upgrade`；HTTP/2 上的 WebSocket（RFC 8441 扩展 CONNECT）未桥接。gRPC 走明文 h2c 接网关（网关出向不做 TLS 终结）。
- 上游只有 rpc:1 时，网关对流式请求体做缓冲（上限 `max_body_bytes`，默认 64 MiB）。
- 未测：macOS（`libbeep.dylib` 路径已写但没有 Mac 可跑）、Delphi（Pascal 绑定只用 Free Pascal 测过）、Flutter（Dart 绑定只在 Dart VM 上测过）。
- Git Bash 用户注意：`beepgw call ... GET /path` 中的 `/path` 会被 MSYS 改写成 Windows 路径，请设 `MSYS_NO_PATHCONV=1`。
