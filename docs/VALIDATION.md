# 本次验证记录

日期：2026-11-03。环境：Windows 11 x64，Rust 1.98.1，全部语言工具链均已安装（无跳过项）。

| 检查 | 结果 |
|---|---|
| `cargo test --workspace --locked`（core 目录） | 通过 |
| `cargo clippy -p minta-gateway --all-targets --locked -- -D warnings` | 通过 |
| `cargo fmt -p minta-gateway -- --check` | 通过 |
| `python tests/standard_test.py --strict`，连续 5 次 | 每次 18 组通过，0 失败，0 跳过 |
| `python tests/run_all.py --build` | 72 组通过，0 失败，0 跳过 |

标准链路覆盖：普通 HTTP、已知/未知长度上传、二进制、Unicode/null JSON、重复头、trailer、提前拒绝上传、截断应答、大小限制、总截止时间、无限流取消、单并发额度恢复、64 个并发调用、两个方向的慢读不阻塞无关调用、点段路径拒绝、TOML 路由（按整段匹配前缀）、OpenAPI、健康检查、内部 TLS/CA/Bearer、HTTPS 后端、真实 gRPC 四种调用模式和 WebSocket。

标准 HTTP/JSON 客户端实测：Python、Node、Java、Go、C#、PowerShell，以及 curl。它们不引用任何 MINTA 客户端库。全语言接入通过同一 HTTP 接口实现。

兼容回归（`run_all.py`）覆盖原 BEEP 矩阵的全部 71 组：原生库、C ABI 绑定、网关 SDK、JSON-Lines、入向网关、rpc:2（gRPC/WebSocket/80 MiB 上传/HTTPS 后端/SASL）及 4 × 6 跨语言互通，外加新网关的标准链路一组。

本轮修复：

- 网关间所有调用共用一条 HTTP/2 连接，原先沿用 hyper 默认窗口（客户端单流 2 MB / 连接 5 MB，服务端各 1 MB）。3 个读得慢的下载或 4 个读得慢的上传就能耗尽连接窗口，使其它调用卡到截止时间（实测 28 s / 6 s）。现在每一跳的单流窗口为 1 MiB，连接窗口覆盖全部并发流之和。
- `/early-reject` 测试后端回 403 后不读上传即关闭，Windows 发 RST 丢弃了 403，约 5% 的运行得到 503。测试后端改为回应后读完上传再关闭。
- 路由前缀改为按整段路径匹配（`/api` 不再匹配 `/apiary`）。
- 含 `.`/`..` 段（含 `%2e` 编码）的目标返回 400，不能越出后端基础路径。

Linux：本轮未在 Linux 上跑完整矩阵。CI 在 ubuntu-latest 与 windows-latest 上运行 `cargo test --workspace` 与 `standard_test.py --strict`，以 CI 结果为准。

兼容行为：BEEP 无法单独取消繁忙通道，后台 drain 超时会中止整个 BEEP 会话；新 gRPC 网关取消独立的流。长连接需调高新网关两侧的总截止时间配置。
