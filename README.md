# MINTA BEEP Bridge

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

- **BEEP 层**：完整实现帧格式、SEQ 滑动窗口流控、通道管理（start/close/greeting）、MSG/RPY/ERR/ANS/NUL、TLS 调谐 profile。
- **RPC profile `urn:minta:beep:rpc:1`**：在 BEEP 通道上承载 HTTP 语义请求（方法、目标、头、二进制体、状态码、流式应答）。
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

一条命令构建并运行全部测试：Rust 核心单元/集成测试、6 个原生库各自的全套用例（与 Rust 服务端互通 + 本语言自通 + 极小窗口大消息 + 流水线 + ANS 流 + 中止 + ERR + 超时回收 + TLS 双向）、14 个 FFI 绑定、13 个网关 SDK、4 种 JVM 语言、JSON-Lines、入向链路，以及 **4 种客户端 × 6 种服务端** 的跨语言矩阵。找不到工具链的语言自动跳过。

最近一次结果：

| 平台 | 结果 | 工具链 |
|---|---|---|
| Ubuntu 24.04（WSL2） | **66 通过，0 失败，0 跳过** | Rust 1.99 / Python 3.12 / Node 18 / JDK 21 / .NET 8 / Go 1.22 / PHP 8.3 / Ruby 3.2 / Zig 0.17 / GCC 13，以及 LuaJIT、Swift 6.4、Nim、Dart 3.13、Julia 1.13、FPC、LDC、GnuCOBOL、R、Elixir、Erlang、GHC、Crystal、Lua、OCaml、Kotlin、Scala 3、Groovy、Clojure |
| Windows 11 | **47 通过，0 失败**（缺工具链的 19 项跳过） | Rust 1.98 / Python 3.13 / Node 24 / JDK 21 / .NET 10 / Go 1.27 / PHP 8.5 / Ruby 4.0 / Zig 0.17 / GCC 16 |

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
tests/           run_all.py（总测试）backend.py clients/
```

## 已知限制

- **SASL** 未实现（RFC 3080 §3.2）；认证建议走 TLS 客户端证书或应用层头。
- **TLS**：采用标准 TLS 调谐 profile，在会话开始时协商；监听端配置 TLS 后要求所有会话先走 TLS。网关入向的 **HTTP 后端**只支持 `http://`（同机/内网部署，或前置 TLS 终结）。
- 请求体在网关和客户端侧**整包发送**（默认上限 64 MiB）；响应体支持流式（ANS）。
- 不桥接 HTTP **trailers**，因此 gRPC 不能经网关透传（gRPC 依赖 trailers）；WebSocket 未桥接。
- 未测：macOS（`libbeep.dylib` 路径已写但没有 Mac 可跑）、Delphi（Pascal 绑定只用 Free Pascal 测过）、Flutter（Dart 绑定只在 Dart VM 上测过）。
- Git Bash 用户注意：`beepgw call ... GET /path` 中的 `/path` 会被 MSYS 改写成 Windows 路径，请设 `MSYS_NO_PATHCONV=1`。
