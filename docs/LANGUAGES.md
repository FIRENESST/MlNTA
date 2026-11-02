# 语言覆盖矩阵

三条接入路径，覆盖面从“深”到“广”：

| 路径 | 原理 | 适合 |
|---|---|---|
| **A. 原生实现** | 该语言纯代码实现完整 BEEP（帧、流控、通道、TLS）+ RPC profile | 主流后端语言，零外部依赖 |
| **B. C ABI（libbeep）** | Rust 核心编译成 `beep.dll` / `libbeep.so` / `libbeep.dylib`，各语言用 FFI 调用 | 任何能调 C 的语言 |
| **C. 网关** | `beepgw` 把 HTTP/1.1、h2c、TCP JSON-Lines 翻译成 BEEP | 任何能发 HTTP（或开 TCP 套接字）的语言，零代码 |

另有 **入向（ingress）**：`beepgw expose` 把任何语言写的现成 HTTP 服务挂到 BEEP 网络上，后端不改一行代码。

图例：✅ 已在本仓库测试中实际编译运行并通过 · ⚪ 已编写，本机缺工具链未验证

## A. 原生实现

| 语言 | 位置 | 客户端 | 服务端 | TLS | 框架适配 | 状态 |
|---|---|---|---|---|---|---|
| Rust | `core/beep` | ✓ 连接池 | ✓ | ✓ rustls | — | ✅ |
| Python 3.8+ | `native/python` | ✓ asyncio + 同步 | ✓ | ✓（3.11+） | ASGI（FastAPI/Starlette/Django ASGI）、WSGI（Flask/Django） | ✅ |
| Node.js 18+ / TypeScript | `native/node` | ✓ | ✓ | ✓ | `(req,res)` 监听器：Express / Koa / Connect / 原生 http | ✅ |
| Java 11+ | `native/java` | ✓ | ✓ | ✓ PEM | — | ✅ |
| C# (.NET 8+) | `native/dotnet` | ✓ async | ✓ | ✓ SslStream | — | ✅ |
| F# / VB.NET | 复用 .NET 库 | ✓ | ✓ | ✓ | — | ✅（`examples/fsharp`、`examples/vbnet`） |
| PowerShell | 复用 .NET 库 | ✓ | — | ✓ | — | 通过 `Add-Type` 使用；网关 SDK ✅ |
| Go 1.21+ | `native/go` | ✓ | ✓ | ✓ crypto/tls | `http.Handler`（Gin/Echo/Chi/mux）、`http.RoundTripper`（任何用 `http.Client` 的代码） | ✅（含 `-race`） |
| Kotlin / Scala / Groovy / Clojure | 复用 Java 库 | ✓ | ✓ | ✓ | — | ⚪（`examples/jvm`） |

## B. C ABI（`core/ffi/include/beep.h`）

| 语言 | 位置 | 客户端 | 服务端回调 | 状态 |
|---|---|---|---|---|
| C | `bindings/c` | ✓ | ✓ | ✅ |
| C++17 | `bindings/cpp/beep.hpp`（header-only RAII） | ✓ | ✓ `std::function` | ✅ |
| Fortran 2003 | `bindings/fortran` | ✓ | — | ✅ |
| Ruby | `bindings/ruby`（Fiddle，标准库） | ✓ | —（服务端用 expose） | ✅ |
| PHP 7.4+ | `bindings/php`（ext-ffi） | ✓ | — | ✅ |
| Zig | `bindings/zig` | ✓ | — | ✅（Zig 0.17） |
| LuaJIT | `bindings/luajit` | ✓ | — | ⚪ |
| Swift | `bindings/swift` | ✓ | — | ⚪ |
| Nim | `bindings/nim` | ✓ | — | ⚪ |
| Dart / Flutter | `bindings/dart` | ✓ | — | ⚪ |
| Julia | `bindings/julia` | ✓ | — | ⚪ |
| Free Pascal / Delphi | `bindings/pascal` | ✓ | — | ⚪ |
| D | `bindings/d` | ✓ | — | ⚪ |
| COBOL（GnuCOBOL） | `bindings/cobol` | ✓ | — | ⚪ |
| Objective-C、Ada、Haskell、OCaml、V、Odin… | 直接用 `beep.h` 走各自 C FFI | ✓ | ✓ | — |

## C. 网关（`beepgw proxy` / `beepgw run`）

**任何能发 HTTP 请求的语言都可以直接用自己的 HTTP 客户端**，SDK 只是加了错误码解析和便捷参数。

| 语言 | 位置 | 状态 |
|---|---|---|
| Shell（curl） | `sdk/shell` | ✅ |
| PowerShell | `sdk/powershell` | ✅ |
| Perl | `sdk/perl`（HTTP::Tiny，核心模块） | ✅ |
| Tcl | `sdk/tcl` | ✅ |
| PHP（无 FFI 环境） | `sdk/php` | ✅ |
| Ruby（无 FFI 环境） | `sdk/ruby` | ✅ |
| TCP JSON-Lines | 协议见 `docs/PROTOCOL.md §3.2` | ✅ |
| R | `sdk/r` | ⚪ |
| Elixir | `sdk/elixir`（OTP `:httpc`） | ⚪ |
| Erlang | `sdk/erlang` | ⚪ |
| Haskell | `sdk/haskell` | ⚪ |
| Crystal | `sdk/crystal` | ⚪ |
| Lua 5.x | `sdk/lua`（LuaSocket） | ⚪ |
| OCaml | `sdk/ocaml` | ⚪ |
| 其它（Kotlin/Swift/Dart 的 HTTP 客户端、ABAP、MATLAB、LabVIEW、Excel VBA……） | 直接发 HTTP 到网关 | — |

## 入向：现有后端零改动挂到 BEEP

```bash
beepgw expose --listen 0.0.0.0:10288 --backend http://127.0.0.1:3000
```

适用于任何 HTTP 服务：Express / NestJS / Koa、Spring Boot / Quarkus / Micronaut、ASP.NET Core、
Django / Flask / FastAPI、Rails / Sinatra、Laravel / Symfony、Gin / Echo / Fiber、Actix / Axum、
Phoenix、Ktor、Vapor……

已实测：Python `http.server` 后端（普通响应 + chunked 流式响应 → ANS）。

Python、Node、Go 还可以**进程内**直接服务 BEEP（无需网关），见上表“框架适配”列。

## 跨语言互通矩阵（`tests/run_all.py` 实测）

客户端 × 服务端全部通过（echo、ANS 流、2 MB 消息、ERR 码）：

| 客户端 ↓ / 服务端 → | Rust | Python | Node | Java | .NET | Go |
|---|---|---|---|---|---|---|
| Rust | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Python | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Node | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Go | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

各原生库还分别与 Rust 做了 TLS 双向互通（客户端→Rust、本语言→本语言）。
