# 本次验证记录

日期：2026-11-03。环境：Windows x64，Rust 1.98.1、Python 3.14.7、Go 1.27.3、.NET SDK 10.0.401。Go 工具链下载到 `tests/.build` 并核对官方 SHA-256；Python 测试依赖安装在仓库 `.venv`。

| 检查 | 结果 |
|---|---|
| `cargo test --workspace --locked`（core 目录） | 26 个测试通过，含文档测试 |
| `cargo clippy -p minta-gateway --all-targets --locked -- -D warnings` | 通过 |
| `cargo fmt -p minta-gateway -- --check` | 通过 |
| `python tests/standard_test.py --strict` | 17 组通过，0 失败，0 跳过 |
| `python tests/run_all.py --build --profile debug`，修复后复跑 `--profile debug` | 44 组通过，0 失败，27 组工具链缺失跳过 |
| `git diff --check`、Python 语法编译 | 通过 |

标准链路覆盖：普通 HTTP、已知/未知长度上传、二进制、Unicode/null JSON、重复头、trailer、提前拒绝上传、截断应答、大小限制、总截止时间、无限流取消、单并发额度恢复、64 个并发调用、TOML 路由、OpenAPI、健康检查、内部 TLS/CA/Bearer、HTTPS 后端、真实 gRPC 四种调用模式和 WebSocket。

标准 HTTP/JSON 客户端实测：Python、Node、Java、Go、C#、PowerShell，以及 curl。它们不引用任何 MINTA 客户端库。全语言接入通过同一 HTTP 接口实现，不意味着逐一编译了所有语言工具链。

兼容回归覆盖：Rust、Python、Node、Java、.NET、F#、VB.NET、Go，C/C++/Fortran FFI，PowerShell SDK，JSON-Lines、BEEP 入向网关、80 MiB 上传、SASL、gRPC/WebSocket 及 4 种客户端 × 6 种服务端的 24 组互通。

缺失工具链的跳过项：Ruby、PHP、Zig、LuaJIT、Nim、D/LDC、Free Pascal、Julia、Dart、Swift、COBOL，Shell、Perl、Tcl、R、Elixir、Erlang、Haskell、Crystal、Lua、OCaml，以及 Kotlin、Scala、Groovy、Clojure。重复列出的 FFI/SDK 路径分别计数。

本机没有验证新网关的 Linux/macOS 运行结果。已添加 Linux/Windows CI 工作流，远端运行结果需以实际 CI 为准。

兼容行为：BEEP 无法单独取消繁忙通道，后台 drain 超时会中止整个 BEEP 会话；新 gRPC 网关取消独立的流。长连接需调高新网关两侧的总截止时间配置。
