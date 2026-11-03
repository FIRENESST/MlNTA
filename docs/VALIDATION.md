# 本次验证记录

日期：2026-11-03。Windows 11 x64（Rust 1.98.1）与 Ubuntu 24.04 / WSL2（Rust 1.99.1），两边都装齐了全部语言工具链，无跳过项。

| 检查 | Windows | Linux |
|---|---|---|
| `cargo test --workspace --locked`（core 目录，含计时器单元测试） | 通过 | 通过 |
| `cargo clippy -p minta-gateway --all-targets --locked -- -D warnings` | 通过 | 通过 |
| `cargo fmt -p minta-gateway -- --check` | 通过 | — |
| `standard_test.py --strict`，debug 版 mintagw | 5/5 次，每次 21 组通过 | 3/3 次，每次 21 组通过 |
| `standard_test.py --strict`，release 版 mintagw | 5/5 次，每次 21 组通过 | 完整矩阵内 1 次通过 |
| `run_all.py --build` | 72 组通过，0 失败，0 跳过 | 72 组通过，0 失败，0 跳过 |

标准链路覆盖：普通 HTTP、已知/未知长度上传、二进制、Unicode/null JSON、重复头、trailer、提前拒绝上传（含 8 MiB 上传未结束时应答已完成）、截断应答、大小限制、无进展超时（慢速上传和持续流动的响应可超过它）、空闲超时、最长时长、请求头超时、SIGTERM/Ctrl-Break 优雅关停、无限流取消、单并发额度恢复、64 个并发调用、两个方向的慢读不阻塞无关调用、点段路径拒绝、TOML 路由（按整段匹配前缀）、OpenAPI、健康检查、内部 TLS/CA/Bearer、HTTPS 后端、真实 gRPC 四种调用模式和 WebSocket（含静默短于空闲超时的隧道保持、超过时关闭）。

标准 HTTP/JSON 客户端实测：Python、Node、Java、Go、C#、PowerShell，以及 curl。它们不引用任何 MINTA 客户端库。全语言接入通过同一 HTTP 接口实现。

兼容回归（`run_all.py`）覆盖原 BEEP 矩阵的全部 71 组：原生库、C ABI 绑定、网关 SDK、JSON-Lines、入向网关、rpc:2（gRPC/WebSocket/80 MiB 上传/HTTPS 后端/SASL）及 4 × 6 跨语言互通，外加新网关的标准链路一组。

修复记录：

- 网关间所有调用共用一条 HTTP/2 连接，原先沿用 hyper 默认窗口（客户端单流 2 MB / 连接 5 MB，服务端各 1 MB）。3 个读得慢的下载或 4 个读得慢的上传就能耗尽连接窗口，使其它调用卡到截止时间（实测 28 s / 6 s）。现在每一跳的单流窗口为 1 MiB，连接窗口覆盖全部并发流之和。
- 应答先于上传结束时（提前拒绝等），对端关闭请求流使上传失败，上传失败又取消了仍在投递的应答，客户端得到中断的连接或错误状态（release 版约 1/4）。现在请求流被对端关闭时上传只停止，不取消调用；入口继续读取并丢弃剩余上传（最多 5 秒、`max_body_bytes` 字节），避免关闭连接时重置导致客户端读不到应答。
- `/early-reject` 测试后端回 403 后不读上传即关闭，Windows 发 RST 丢弃了 403，约 5% 的运行得到 503。测试后端改为回应后读完上传再关闭。
- 路由前缀改为按整段路径匹配（`/api` 不再匹配 `/apiary`）。
- 含 `.`/`..` 段（含 `%2e` 编码）的目标返回 400，不能越出后端基础路径。
- `timeout_ms` 默认值由 30000 改为 60000（命令行与 TOML 共用同一默认值，有单元测试）。
- 超时由“调用及其流的总时长”改为按进展计：`timeout_ms` 是收到应答头前的无进展时间，`idle_timeout_ms`（默认 300000）是之后两个方向都无数据的时间，`max_duration_ms` 为可选硬上限。原先 WebSocket、长流和超过 60 秒的上传到点必断；网关间也不再发送会截断长上传的 `grpc-timeout`。
- 被取消的调用中，网关内部转发上传/应答的 body 改为报错结束；原先会正常结束，后端可能把截断的分块上传当成完整请求。
- 入口的 HTTP/1.1 请求头读取超时（`header_timeout_ms`，默认 30000）原先因缺少计时器而不生效，现已生效。
- 收到 SIGINT/SIGTERM（Windows 上 Ctrl-C/Ctrl-Break）时停止接受连接、`/readyz` 返回 draining，在途调用有 `shutdown_grace_ms`（默认 20000）完成后以 0 退出；原先只处理 Ctrl-C 且直接中止在途调用。accept 出错（如文件描述符耗尽）不再使网关退出。
- BEEP 路径标为弃用（维护期），`beepgw` 长驻命令启动时警告，迁移指南见 MIGRATION.md。

兼容行为：BEEP 无法单独取消繁忙通道，后台 drain 超时会中止整个 BEEP 会话；新 gRPC 网关取消独立的流。长时间静默的 WebSocket 需要应用心跳或调高两侧 `idle_timeout_ms`。
