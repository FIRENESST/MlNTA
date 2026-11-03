# 语言覆盖矩阵

## 推荐：标准 HTTP/JSON（所有语言）

新入口 `mintagw` 对应用提供普通 HTTP 或 `POST /v1/call`，网关间使用 gRPC。下表所有语言都可以使用自身 HTTP 库或调用 curl，不需要维护专用协议栈，也不要求该语言具有 gRPC/QUIC 绑定。后端侧任意现有 HTTP 服务无需修改。

`examples/http/` 给出 Python、Node、Java、Go、C#、PowerShell 的无 MINTA 依赖客户端。`tests/standard_test.py` 报告可用工具链的实测结果与跳过项。接口见 [HTTP_GATEWAY.md](HTTP_GATEWAY.md) 与 [OpenAPI](HTTP_API.openapi.json)。

## 历史 BEEP 兼容路径（已弃用）

> 这些路径只修缺陷和安全问题，计划在之后的主版本移除；迁移见 [MIGRATION.md](MIGRATION.md)。

下方平台通过标记来自原 BEEP 版本的测试记录，不代表新 gRPC 网关的测试结果；这些旧路径继续保留。

三条接入路径，覆盖面从“深”到“广”：

| 路径 | 原理 | 适合 |
|---|---|---|
| **A. 原生实现** | 该语言纯代码实现完整 BEEP（帧、流控、通道、TLS）+ RPC profile | 主流后端语言，零外部依赖 |
| **B. C ABI（libbeep）** | Rust 核心编译成 `beep.dll` / `libbeep.so` / `libbeep.dylib`，各语言用 FFI 调用 | 任何能调 C 的语言 |
| **C. 网关** | `beepgw` 把 HTTP/1.1、h2c、TCP JSON-Lines 翻译成 BEEP | 任何能发 HTTP（或开 TCP 套接字）的语言，零代码 |

另有 **入向（ingress）**：`beepgw expose` 把任何语言写的现成 HTTP 服务挂到 BEEP 网络上，后端不改一行代码。

图例：✅ 已在 `tests/run_all.py` 中实际编译运行并通过；**W** = Windows 11，**L** = Ubuntu 24.04（WSL2）。

## A. 原生实现

| 语言 | 位置 | 客户端 | 服务端 | TLS | 框架适配 | 状态 |
|---|---|---|---|---|---|---|
| Rust | `core/beep` | ✓ 连接池 | ✓ | ✓ rustls | — | ✅ W L |
| Python 3.8+ | `native/python` | ✓ asyncio + 同步 | ✓ | ✓（3.11+） | ASGI（FastAPI/Starlette/Django ASGI）、WSGI（Flask/Django） | ✅ W L |
| Node.js 18+ / TypeScript | `native/node` | ✓ | ✓ | ✓ | `(req,res)` 监听器：Express / Koa / Connect / 原生 http | ✅ W L |
| Java 11+ | `native/java` | ✓ | ✓ | ✓ PEM | — | ✅ W L |
| C# (.NET 8+) | `native/dotnet` | ✓ async | ✓ | ✓ SslStream | — | ✅ W L |
| F# / VB.NET | 复用 .NET 库 | ✓ | ✓ | ✓ | — | ✅ W L（`examples/fsharp`、`examples/vbnet`） |
| PowerShell | 复用 .NET 库 | ✓ | — | ✓ | — | 通过 `Add-Type` 使用；网关 SDK ✅ W L |
| Go 1.21+ | `native/go` | ✓ | ✓ | ✓ crypto/tls | `http.Handler`（Gin/Echo/Chi/mux）、`http.RoundTripper`（任何用 `http.Client` 的代码） | ✅ W L（W 含 `-race`） |
| Kotlin / Scala 3 / Groovy / Clojure | 复用 Java 库（`examples/jvm`） | ✓ | ✓ | ✓ | — | ✅ W L（Kotlin 2.4、Scala 3.3、Groovy 6、Clojure 1.12 / 1.11） |

## B. C ABI（`core/ffi/include/beep.h`）

| 语言 | 位置 | 客户端 | 服务端回调 | 状态 |
|---|---|---|---|---|
| C | `bindings/c` | ✓ | ✓ | ✅ W L |
| C++17 | `bindings/cpp/beep.hpp`（header-only RAII） | ✓ | ✓ `std::function` | ✅ W L |
| Fortran 2003 | `bindings/fortran` | ✓ | — | ✅ W L |
| Ruby | `bindings/ruby`（Fiddle，标准库） | ✓ | —（服务端用 expose） | ✅ W L |
| PHP 7.4+ | `bindings/php`（ext-ffi） | ✓ | — | ✅ W L |
| Zig | `bindings/zig` | ✓ | — | ✅ W L（Zig 0.17） |
| LuaJIT | `bindings/luajit` | ✓ | — | ✅ W L（2.1） |
| Swift | `bindings/swift`（SwiftPM，含 `beep-test`） | ✓ | — | ✅ W L（6.4） |
| Nim | `bindings/nim` | ✓ | — | ✅ W L（W 2.4，L 1.6） |
| Dart / Flutter | `bindings/dart`（需 `package:ffi`） | ✓ | — | ✅ W L（Dart 3.13；Flutter 未测） |
| Julia | `bindings/julia` | ✓ | — | ✅ W L（1.13） |
| Free Pascal / Delphi | `bindings/pascal` | ✓ | — | ✅ W L（FPC 3.2；Delphi 未测） |
| D | `bindings/d` | ✓ | — | ✅ W L（LDC：W 1.43，L 1.36） |
| COBOL（GnuCOBOL） | `bindings/cobol` | ✓ | — | ✅ W L（GnuCOBOL：W 3.2，L 3.1） |
| Objective-C、Ada、Haskell、OCaml、V、Odin… | 直接用 `beep.h` 走各自 C FFI | ✓ | ✓ | — |

## C. 网关（`beepgw proxy` / `beepgw run`）

**任何能发 HTTP 请求的语言都可以直接用自己的 HTTP 客户端**，SDK 只是加了错误码解析和便捷参数。

| 语言 | 位置 | 状态 |
|---|---|---|
| Shell（curl） | `sdk/shell` | ✅ W L |
| PowerShell | `sdk/powershell` | ✅ W L |
| Perl | `sdk/perl`（HTTP::Tiny，核心模块） | ✅ W L |
| Tcl | `sdk/tcl` | ✅ W L |
| PHP（无 FFI 环境） | `sdk/php` | ✅ W L |
| Ruby（无 FFI 环境） | `sdk/ruby` | ✅ W L |
| TCP JSON-Lines | 协议见 `docs/PROTOCOL.md §3.2` | ✅ W L |
| R | `sdk/r`（`curl` 包） | ✅ W L（R：W 4.6，L 4.3） |
| Elixir | `sdk/elixir`（OTP `:httpc`） | ✅ W L（W 1.20，L 1.14） |
| Erlang | `sdk/erlang`（`inets`） | ✅ W L（OTP：W 29，L 25） |
| Haskell | `sdk/haskell`（http-client） | ✅ W L（GHC：W 9.10，L 9.4） |
| Crystal | `sdk/crystal`（标准库） | ✅ W L（1.22） |
| Lua 5.x | `sdk/lua`（LuaSocket） | ✅ W L（5.4） |
| OCaml | `sdk/ocaml`（cohttp-lwt-unix，链接需 `-thread`） | ✅ W L（W 5.4 + cohttp 6.3，L 4.14 + cohttp 5.3） |
| 其它（Kotlin/Swift/Dart 的 HTTP 客户端、ABAP、MATLAB、LabVIEW、Excel VBA……） | 直接发 HTTP 到网关 | — |

## 协议级桥接（任何语言，经网关）

| 能力 | 用法 | 状态 |
|---|---|---|
| **gRPC**（unary / 服务端流 / 客户端流 / 双向流、metadata、trailer、状态码、deadline） | gRPC 客户端明文 h2c 连 `beepgw proxy`；对端 `beepgw expose --backend h2c://…` | ✅ W L（grpcio ↔ grpcio，W 1.80、L 1.51） |
| **WebSocket**（HTTP/1.1 Upgrade，握手端到端） | `ws://网关/…`；对端 `beepgw expose --backend http://…` | ✅ W L（websockets W 16、L 10.4） |
| 流式上传（长度未知、不限大小） | 普通 HTTP 分块上传 | ✅ W L（80 MiB + CRC 校验） |
| SASL PLAIN 认证 | `--sasl-user` / `--sasl-users`，身份以 `X-Beep-User` 到后端；C ABI `beep_client_new_auth` | ✅ W L |
| HTTPS 后端 | `beepgw expose --backend https://… [--backend-ca …]` | ✅ W L |

这些能力依赖 `urn:minta:beep:rpc:2`（docs/PROTOCOL.md §2.6）：网关与 Rust 核心之间自动协商；原生库仍以 rpc:1 互通。

## 入向：现有后端零改动挂到 BEEP

```bash
beepgw expose --listen 0.0.0.0:10288 --backend http://127.0.0.1:3000
```

适用于任何 HTTP 服务：Express / NestJS / Koa、Spring Boot / Quarkus / Micronaut、ASP.NET Core、
Django / Flask / FastAPI、Rails / Sinatra、Laravel / Symfony、Gin / Echo / Fiber、Actix / Axum、
Phoenix、Ktor、Vapor……

已实测：Python `http.server` 后端（普通响应 + chunked 流式响应 → ANS）。

Python、Node、Go 还可以**进程内**直接服务 BEEP（无需网关），见上表“框架适配”列。

## 跨语言互通矩阵（`tests/run_all.py` 实测，Windows 与 Linux 均通过）

客户端 × 服务端全部通过（echo、ANS 流、2 MB 消息、ERR 码）：

| 客户端 ↓ / 服务端 → | Rust | Python | Node | Java | .NET | Go |
|---|---|---|---|---|---|---|
| Rust | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Python | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Node | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Go | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

各原生库还分别与 Rust 做了 TLS 双向互通（客户端→Rust、本语言→本语言）。
