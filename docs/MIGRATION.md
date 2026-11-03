# 从 BEEP 迁移到 mintagw

BEEP 路径已弃用：`beepgw`、`native/` 下的各语言 BEEP 库、C ABI（`core/ffi`）及 `bindings/`、面向 beepgw 的 `sdk/`，以及协议 `urn:minta:beep:rpc:1/2`（[PROTOCOL.md](PROTOCOL.md)）。新部署请使用 `mintagw`（[HTTP_GATEWAY.md](HTTP_GATEWAY.md)）。

## 时间表

| 阶段 | 内容 |
|---|---|
| 现在：维护期 | 文档标为弃用；`beepgw run/proxy/expose` 启动时打印弃用警告。只修缺陷和安全问题，不加功能。现有部署继续可用。 |
| 下一步 | 各语言 BEEP 库在运行时给出弃用提示；补齐下文“尚无对应”中需要保留的能力。 |
| 移除 | 在之后的某个主版本删除 BEEP 代码。删除前会在发布说明中提前公告。 |

两套网关可以同时运行（默认端口不同：BEEP 10288，gRPC 10290），按服务逐个迁移即可。

## 部署对照

```sh
# 之前
beepgw expose --listen 0.0.0.0:10288 --backend http://127.0.0.1:3000
beepgw proxy  --listen 127.0.0.1:8080 --upstream 10.0.0.5:10288
# 之后
mintagw expose --listen 0.0.0.0:10290 --backend http://127.0.0.1:3000
mintagw proxy  --listen 127.0.0.1:8080 --upstream http://10.0.0.5:10290
```

应用这一侧通常不用改：仍然向 `proxy` 的同一地址、同一路径发普通 HTTP。注意 `beepgw expose` 默认监听 `0.0.0.0`，`mintagw expose` 默认只监听 `127.0.0.1`，跨机器部署时要显式指定。

| beepgw | mintagw | 说明 |
|---|---|---|
| `proxy --upstream host:port` | `proxy --upstream http://host:port` | 上游是 gRPC 地址；TLS 用 `https://`。 |
| `--sessions`、`--max-channels` | `--max-in-flight` | 网关间只用一条 HTTP/2 连接，按流并发。 |
| `--timeout-ms`（等应答头） | `--timeout-ms` + `--idle-timeout-ms` + `--max-duration-ms` | 见 HTTP_GATEWAY.md“限额、超时与取消”。 |
| `--tls-cert/--tls-key`（expose） | `--cert-file/--key-file` | 标准 TLS，不再走 BEEP TLS 调谐。 |
| `--tls-server-name`、`--tls-ca`（proxy） | `--server-name`、`--ca-file` | |
| `--sasl-user/--sasl-password`（proxy） | `--token-env`（两侧相同） | 网关之间用 Bearer token 互认，不再区分用户。 |
| `--sasl-users FILE`（expose） | `--token-env` | 同上。 |
| `--backend-ca` | `--backend-ca-file` | |
| `--jsonl ADDR` | 无 | 改用 `POST /v1/call`（JSON 请求/应答）。 |
| `run -c beepgw.toml` | `run -c mintagw.toml` | 配置格式不同，见 `core/bridge/mintagw.example.toml`。 |

## HTTP 接口对照

| beepgw | mintagw |
|---|---|
| `X-Beep-Upstream`、`X-Beep-Timeout-Ms` | `X-Minta-Upstream`、`X-Minta-Timeout-Ms`（旧名称仍然接受） |
| 出错时 `X-Beep-Error-Code` 与 BEEP 错误码 | 标准 HTTP 状态与 `{"error":{"code":"...","message":"..."}}` |
| `GET /__beep/health` | `GET /readyz`（`/__beep/health` 仍然应答） |
| 后端收到 `X-Beep-User`（SASL 身份） | 无；最终用户认证放在应用或反向代理 |
| `GET /__beep/stats`、`/__beep/metrics` | 暂无 |
| `POST /__beep/raw` | 无（BEEP 专用） |

## 客户端与服务端代码

- **各语言 BEEP 客户端**（`native/` 的 Python、Node、Java、.NET、Go 客户端，`bindings/` 经 C ABI 的各语言，`sdk/`）：改用语言自带的 HTTP 客户端，直接请求 `proxy`，或调用 `POST /v1/call`。示例见 `examples/http/`。`sdk/` 发出的普通请求不用改也能用，但 BEEP 错误码和 SASL 参数不再有意义。
- **进程内 BEEP 服务端**（Python ASGI/WSGI、Node、Go `http.Handler` 适配）：把应用作为普通 HTTP 服务运行，用 `mintagw expose --backend` 接入。
- **gRPC、WebSocket、流式上传/下载、trailer**：经 `mintagw` 照常工作，无需改代码。

## 尚无对应

以下 beepgw 能力 mintagw 目前没有：Prometheus 指标（`/__beep/metrics`）、JSON-Lines TCP 入口、按用户的身份传递（SASL → `X-Beep-User`）、`beepgw call` 命令行（可用 curl）、`beepgw echo` 测试服务、原始 BEEP 帧（`/__beep/raw`）。依赖这些能力的部署暂时继续使用 beepgw；移除 BEEP 之前，需要保留的能力（例如指标）应先在 mintagw 中补齐。
