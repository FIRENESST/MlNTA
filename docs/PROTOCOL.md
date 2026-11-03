# MINTA BEEP Bridge — 协议规范

> **已弃用。** BEEP 路径只修缺陷和安全问题，计划在之后的主版本移除。新部署使用 mintagw，迁移见 [MIGRATION.md](MIGRATION.md)。

本文是 BEEP 兼容实现（Rust BEEP 核心、beepgw、C ABI、各语言原生 BEEP 库及旧 SDK）的规范。推荐的新入口 mintagw 使用标准 HTTP/JSON 与 gRPC，见 [HTTP_GATEWAY.md](HTTP_GATEWAY.md)、[OpenAPI](HTTP_API.openapi.json) 和 [bridge.proto](../core/bridge/proto/bridge.proto)。
分三层：

1. **BEEP 传输层** —— RFC 3080（BEEP Core）+ RFC 3081（BEEP over TCP）的实现子集与约定
2. **RPC Profile** —— 跑在 BEEP 通道上的、承载“后端请求”的自定义 profile：`urn:minta:beep:rpc:1`，及其超集 `urn:minta:beep:rpc:2`（流式请求体、全双工、trailer、协议升级，§2.6）
3. **网关接入协议** —— 让任何语言不写一行 BEEP 代码即可接入：HTTP 透传、TCP JSON-Lines、原始 BEEP 消息透传

---

## 1. BEEP 传输层

### 1.1 帧格式（RFC 3080 §2.2）

```
MSG SP channel SP msgno SP more SP seqno SP size CRLF  <payload>  END CRLF
RPY SP channel SP msgno SP more SP seqno SP size CRLF  <payload>  END CRLF
ERR SP channel SP msgno SP more SP seqno SP size CRLF  <payload>  END CRLF
ANS SP channel SP msgno SP more SP seqno SP size SP ansno CRLF  <payload>  END CRLF
NUL SP channel SP msgno SP more SP seqno SP size CRLF  END CRLF          (size 必须为 0, more 必须为 '.')
SEQ SP channel SP ackno SP window CRLF                                   (RFC 3081 §3.1)
```

| 字段 | 范围 | 说明 |
|---|---|---|
| channel | 0..2147483647 | 0 为管理通道 |
| msgno | 0..2147483647 | 同一通道上未完成应答的 MSG 之间必须互不相同 |
| more | `.` / `*` | `*` 表示该消息还有后续帧 |
| seqno | 0..4294967295 | 本帧首个载荷字节在“本方向、本通道”字节流中的序号，模 2^32 |
| size | 0..2147483647 | 载荷字节数（不含头行和 `END\r\n`） |
| ansno | 0..2147483647 | 同一 MSG 的多个 ANS 之间互不相同 |

**实现约定**

- 头行最长 128 字节，超过即视为畸形帧。
- 任何畸形帧（头行语法错、seqno 不连续、size 不符、缺少 `END\r\n`、超出接收窗口、应答对应不存在的 msgno、续帧的类型/msgno/ansno 与前帧不一致）→ **直接断开 TCP**，不发送任何应答（RFC 3080 §2.2.1.1）。
- 发送端单帧载荷上限 = `min(对端剩余窗口, max_frame_size)`，`max_frame_size` 默认 **65536**。
- 同一通道上、同一发送方向，**不同消息的帧不交错**（整条消息发完才发下一条）。接收端对同一 MSG 的不同 ansno 的 ANS 交错保持宽容。

### 1.2 载荷：MIME 实体（RFC 3080 §2.2.2）

```
Header-Name: value CRLF
...
CRLF
<body bytes>
```

- 没有实体头时，载荷以一个 `CRLF` 开头。
- 默认 `Content-Type: application/octet-stream`，`Content-Transfer-Encoding: binary`。
- 解析时支持头部折行（以空格/制表符开头的续行）；头名大小写不敏感。
- 宽容解析：若载荷不以 `CRLF` 开头且找不到 `CRLF CRLF`，或首行不含 `:`，则整个载荷视为 body。

### 1.3 流控（RFC 3081 §3.1）

- 每个通道、每个方向各有一个接收窗口，初始 **4096** 字节。
- 接收端发送 `SEQ channel ackno window`：允许对端发送 seqno ∈ `[ackno, ackno+window)` 的字节。
- **本实现的策略**：
  - 通道建立后（通道 0 在收到对端 greeting 后）立即发送 `SEQ ch recv_seq W`，`W` = `recv_window`，默认 **262144**。
  - 字节被上层**真正消费**后才计入；当 `recv_seq - last_advertised_ack ≥ W/2` 时再发一次 SEQ。这样每通道缓冲上限为 W，背压真实有效。
  - 接收到超出已通告窗口的帧 → 协议错误，断开。
  - 收到未知通道的 SEQ → 忽略。
- 所有 seqno 运算按 u32 回绕。

### 1.4 会话与管理通道（channel 0）

**问候（greeting）**：连接建立后双方**立即**在通道 0 发送 `RPY 0 0`：

```
Content-Type: application/beep+xml

<greeting><profile uri='urn:minta:beep:rpc:1'/><profile uri='urn:minta:beep:rpc:2'/></greeting>
```

- 监听方列出它接受的 profile；发起方若没有注册任何 profile 则发 `<greeting/>`。
- 收到对端 greeting 之前不得发送其它消息（SEQ 除外）。
- 双方在通道 0 上发出的第一个 MSG 使用 **msgno 1**（msgno 0 被隐式问候占用）；其它通道 msgno 从 0 起递增，回绕时跳过仍未完成的编号。

**通道编号**：发起方（initiator）只用奇数，监听方（listener）只用偶数。

**开启通道**

```
MSG 0 n:  <start number='1'><profile uri='urn:minta:beep:rpc:1'/></start>
RPY 0 n:  <profile uri='urn:minta:beep:rpc:1'/>
ERR 0 n:  <error code='550'>no acceptable profile</error>
```

- `<start>` 可含多个 `<profile>`，接收方选**第一个**自己支持的；可带 `serverName` 属性（忽略）。
- `<profile>` 内可携带初始化数据（文本/CDATA，`encoding='base64'` 表示 base64）；应答 `<profile>` 也可携带。
- 通道号奇偶不对或已占用 → `ERR 553`。
- 发起端在发送 `<start>` 时就预注册该通道（pending），收到 RPY 后转为 open，收到 ERR 则删除——保证紧随 RPY 到达的 SEQ 不会丢失。

**关闭通道**

```
MSG 0 n:  <close number='1' code='200'/>
RPY 0 n:  <ok/>
ERR 0 n:  <error code='550'>still working</error>
```

- 若该通道仍有任一方向未完成的消息 → `ERR 550`。
- `number='0'` 表示释放整个会话：回 `<ok/>`，发送缓冲刷出后关闭 TCP；发起方收到 `<ok/>` 后关闭 TCP。

**错误码**（RFC 3080 §8）：200 成功；421 服务不可用；450 未执行（暂时）；451 中止；454 临时认证失败；500 语法错误；501 参数语法错误；504 参数未实现；530 需要认证；534 认证机制太弱；535 认证失败；537 未授权；538 需要加密；550 未执行（无权限/无可用 profile/仍在工作）；553 参数无效；554 事务失败。

**XML 约定**：只处理上述元素；属性单双引号均可；识别 `&lt; &gt; &amp; &quot; &apos; &#N; &#xN;` 与 CDATA；**拒绝 DOCTYPE**（防实体扩展攻击）。

### 1.5 消息处理顺序（RFC 3080 §2.6）

- 同一通道上，服务端**按收到顺序**处理 MSG，并**按同一顺序**发出应答。
- 不同通道之间完全并行。
- 因此，客户端想并发请求，应使用多个通道（本项目的 RPC 客户端用“通道池”，每通道同一时刻只跑一个请求，避免队头阻塞）。

### 1.6 TLS 调谐（RFC 3080 §3.1）

profile `http://iana.org/beep/TLS`，在会话开始时协商：

```
C: MSG 0 1:  <start number='1'><profile uri='http://iana.org/beep/TLS'><![CDATA[<ready />]]></profile></start>
S: RPY 0 1:  <profile uri='http://iana.org/beep/TLS'><![CDATA[<proceed />]]></profile>
   …… TLS 握手 ……
   双方在 TLS 之上重新交换 greeting，开始一个全新的会话（之前的会话状态全部作废）
```

- 配置了 TLS 的监听方只接受先走 TLS 的会话；明文会话不会被提供任何其它 profile。
- 发起方用 `server_name` 校验证书（SNI），信任锚默认 Mozilla 根证书，可指定自有 CA 文件。

### 1.7 SASL 认证（RFC 3080 §3.2、RFC 4422）

实现机制：**PLAIN**（RFC 4616），profile `http://iana.org/beep/SASL/PLAIN`。

```
C: MSG 0 n:  <start number='3'><profile uri='http://iana.org/beep/SASL/PLAIN'><![CDATA[<blob>AGFsaWNlAHMzY3JldA==</blob>]]></profile></start>
S: RPY 0 n:  <profile uri='http://iana.org/beep/SASL/PLAIN'><![CDATA[<blob status='complete' />]]></profile>
```

- blob 是 base64 的 `authzid NUL authcid NUL password`；凭据直接搭载在 `<start>` 上，一个往返完成。监听方若没有处理搭载数据，发起方改在该通道上发 `MSG <blob>…</blob>`，成功回 `RPY <blob status='complete' />`。
- 失败：`ERR 535`（认证失败，网关映射 HTTP 401）；`<blob status='abort'/>` 放弃认证。
- 成功后身份绑定到**整个会话**；发起方随即关闭这个 SASL 通道。同一会话重复认证 → `ERR 550`。
- 监听方可配置“必须认证”：认证前开启任何非 SASL 通道都回 `ERR 530`（authentication required，映射 HTTP 401）。
- 只做身份认证，不提供 SASL 安全层；PLAIN 以明文传口令，应与 TLS（§1.6）一起使用——TLS 协商在先，SASL 在 TLS 之后的新会话上进行。
- 认证不允许“代理授权”：authzid 必须为空或等于 authcid。
- 网关入向把会话身份以 `X-Beep-User` 头交给 HTTP 后端（并丢弃对端自己带来的同名头），后端据此做授权。

### 1.8 默认值

| 参数 | 默认值 |
|---|---|
| TCP 端口 | 10288（IANA `beep`） |
| recv_window | 262144 |
| max_frame_size | 65536 |
| max_message_size（整包收集时上限） | 67108864（64 MiB） |
| 每会话最大通道数（RPC 客户端池） | 64 |

---

## 2. RPC Profile：`urn:minta:beep:rpc:1`

在 BEEP 通道上承载 HTTP 语义的请求/响应，从而让任意后端框架无损对接。

### 2.1 请求（MSG）

MIME 实体头：

| 头 | 必需 | 说明 |
|---|---|---|
| `Rpc-Method` | 是 | 方法，如 `GET`/`POST`；也可以是任意 RPC 动词 |
| `Rpc-Target` | 是 | 请求目标，如 `/users/42?x=1`；也可以是 `service/method` |
| `Rpc-Id` | 否 | 关联 ID，应答原样回显 |
| `Rpc-Timeout` | 否 | 截止时间提示（毫秒） |
| `Rpc-Authority` | 否 | 原始 Host |
| `Content-Type` | 否 | body 类型 |
| 其它任意头 | 否 | 应用头，原样透传 |

body 即请求体（二进制原样）。大请求体可分多帧（`more='*'`）流式发送。

### 2.2 普通应答（RPY）

| 头 | 必需 | 说明 |
|---|---|---|
| `Rpc-Status` | 否（缺省 200） | 三位状态码。**后端返回 404/500 也走 RPY**——那是应用层结果，不是传输错误 |
| `Rpc-Reason` | 否 | 原因短语 |
| `Rpc-Id` | 否 | 回显 |
| 其它头 | 否 | 应用响应头 |

### 2.3 流式应答（ANS … NUL）

用于 SSE、分块下载等不定长响应：

- `ANS ansno=0`：响应头实体（含 `Rpc-Status` 等），其 body 是第一段数据（可为空）。
- `ANS ansno=1..N`：无实体头（载荷以 `CRLF` 开头），body 为后续数据块，按 ansno 递增顺序拼接。
- `NUL`：结束。
- **异常中止**：若响应方在流中途出错（如后端连接断开），发送一条带 `Rpc-Abort: <原因>` 头的 ANS，然后 NUL。
  客户端收到后必须把响应视为失败（不得当作完整响应），网关会中断对应的 HTTP 响应。

客户端**必须**同时支持 RPY 与 ANS/NUL 两种应答形式。

### 2.4 传输级失败（ERR）

```
Content-Type: application/beep+xml

<error code='554'>backend connection refused</error>
```

ERR 只表示“请求没能得到应用层应答”（无路由、后端不可达、超时、信封畸形）。网关映射到 HTTP：

| BEEP | HTTP | | BEEP | HTTP |
|---|---|---|---|---|
| 421 | 503 | | 530/534/535 | 401 |
| 450 | 503 | | 537/538 | 403 |
| 451 | 504 | | 550 | 502 |
| 500/501/553 | 400 | | 554 | 502 |
| 504 | 501 | | 其它 | 502 |

若 ERR 载荷不是 `<error>` XML，code 取 554、消息取整个 body 文本。

### 2.5 头部透传规则（HTTP ↔ RPC）

- 剥离逐跳头：`Connection` `Keep-Alive` `Proxy-Connection` `Transfer-Encoding` `TE` `Trailer` `Upgrade` `Proxy-Authorization` `Proxy-Authenticate` `Content-Length`，以及 `Connection` 头里列出的头。
- 例外：`TE: trailers` 原样透传（gRPC 要求它，HTTP/2 也只允许这一个 TE 值）。
- `Host` → `Rpc-Authority`。
- 剥离 `Content-Transfer-Encoding`（BEEP 保留）。
- 客户端发来的 `Rpc-Method` `Rpc-Target` `Rpc-Status` `Rpc-Authority` `Rpc-Reason` `Rpc-Abort` `Rpc-Body` `Rpc-Trailers` `Rpc-Upgrade` 一律剥离由网关重写；允许客户端自带 `Rpc-Id` `Rpc-Timeout`。
- 头值中的 CR/LF 一律剥除。

### 2.6 `urn:minta:beep:rpc:2`：流式请求、全双工、trailer、协议升级

rpc:2 是 rpc:1 的严格超集：rpc:1 的一切规则在 rpc:2 通道上照常成立，下面只列增加的部分。
它让 gRPC（四种调用模式）、WebSocket、任意长度上传都能无损穿过 BEEP。

**2.6.1 协商**

- 监听方在 greeting 里同时列出 rpc:1 与 rpc:2。
- 发起方的 `<start>` 依次提供 `rpc:2`、`rpc:1`；对端按 §1.4 选第一个自己支持的。只懂 rpc:1 的实现（本仓库的各语言原生库）因此自动落到 rpc:1，互通不受影响。
- 通道的 profile 决定该通道上可用的语义；客户端据此决定请求体是流式发送还是整包发送。

**2.6.2 分块请求体（`Rpc-Body: chunked`）**

- 请求头含 `Rpc-Body: chunked` 时，实体头之后的 body 采用 HTTP/1.1 分块编码（RFC 9112 §7.1）：
  `十六进制长度[;扩展] CRLF 数据 CRLF` …… 最后是 `0 CRLF`、零或多行 trailer 字段、`CRLF`。
- 该 MSG 以多帧（`more='*'`）随数据到达逐步发送，长度不必事先知道，也**不受** `max_message_size` 限制（接收方边读边处理）。
- 末尾的 trailer 字段即**请求 trailer**（网关转成 HTTP 请求 trailer）。
- **中止**：MSG 在 `0` 终止块之前就结束（最后一帧 `more='.'`），表示请求方放弃了请求（例如 HTTP 客户端断开、gRPC 取消）。响应方必须把它当作被取消的请求、而不是一个较短的完整请求：若应答尚未开始，回 ERR（建议 `451`）；若已在 ANS 流中，发带 `Rpc-Abort` 的 ANS 后 NUL。网关把它转成对后端的中断请求（HTTP/2 RST_STREAM / 连接中断）。
- 没有 `Rpc-Body` 头时，body 就是普通的完整字节（同 rpc:1）。

**2.6.3 全双工**

- 在 rpc:2 通道上，响应方**可以在请求 MSG 尚未收完时就开始应答**（ANS 或 RPY），请求方必须一边发送请求帧一边读取应答。
- 应答先结束、请求后结束是合法的：响应方继续接收并丢弃剩余的 MSG 帧。
- RFC 3080 的其余约束不变：同方向不同消息的帧不交错；同一通道按顺序处理 MSG。由于 RPC 客户端每通道同时只有一个请求，这一点不构成限制。

**2.6.4 应答 trailer（`Rpc-Trailers`）**

- ANS 流中，实体头含 `Rpc-Trailers: 1` 的那条 ANS 携带**应答 trailer 字段**（其余实体头即 trailer，如 `grpc-status`、`grpc-message`），body 为空。
- 它必须是 NUL 之前的最后一条 ANS。RPY 应答没有 trailer；需要 trailer 的响应方必须用 ANS 流。
- 没有 body 也没有 trailer 的响应（如 gRPC 的 trailers-only 错误响应，状态在响应头里）用一条空 body 的 RPY 表达，网关据此输出单个带 END_STREAM 的 HTTP/2 HEADERS 帧。

**2.6.5 协议升级（`Rpc-Upgrade`）**

- 请求头 `Rpc-Upgrade: <协议>`（如 `websocket`）表示请求升级，且请求体必须是 `Rpc-Body: chunked`。升级相关的端到端头（`Sec-WebSocket-Key`、`Sec-WebSocket-Version`、`Sec-WebSocket-Protocol` 等）作为普通应用头透传，因此握手由真正的客户端与后端完成（`Sec-WebSocket-Accept` 由后端计算）。
- 应答 `Rpc-Status: 101`（ANS 0）表示已切换：之后 ANS 的 body 是“后端 → 客户端”方向的原始字节流，NUL 关闭这个方向；请求体的数据块是“客户端 → 后端”方向的原始字节流，`0` 终止块关闭这个方向。
- 应答不是 101 时就是普通响应（升级被拒绝），请求方应结束请求体。
- rpc:1 通道不支持升级；客户端遇到只有 rpc:1 的对端应报告失败（Rust 实现返回 `ERR 504`，网关映射为 HTTP 501）。

**2.6.6 实现状况**

| 组件 | rpc:2 |
|---|---|
| Rust 核心（`core/beep`）客户端 / 服务端 | ✓（`RpcClient::call_duplex`、`RpcHandler::streaming`、`BodyItem::Trailers`） |
| 网关 `beepgw`（出向 HTTP/h2c、入向） | ✓ |
| C ABI `libbeep` 客户端 | 自动协商 rpc:2；普通调用的线上格式与 rpc:1 相同 |
| 各语言原生库（Python、Node、Java、.NET、Go） | rpc:1（与 rpc:2 对端经 §2.6.1 自动回落互通） |

---

## 3. 网关接入协议（`beepgw`）

### 3.1 HTTP 透传（出向：应用 → BEEP）

监听 HTTP/1.1 与 HTTP/2 明文（h2c）。除保留前缀 `/__beep/` 外，**任何请求**被转换为一次 RPC 调用：

```
POST http://127.0.0.1:8080/orders?id=7   →   MSG  Rpc-Method: POST / Rpc-Target: /orders?id=7 / ...
```

网关控制头（消费掉、不转发）：

| 头 | 说明 |
|---|---|
| `X-Beep-Upstream` | 选择上游（配置中的名字），缺省用监听器绑定的上游 |
| `X-Beep-Timeout-Ms` | 本次超时；同时作为 `Rpc-Timeout` 下发 |

请求：
- 长度已知且不超过 256 KiB 的请求体整包收齐后作为一条 MSG 发送。
- 其余请求——长度未知（分块上传、HTTP/2 无 `content-length`）、gRPC（`content-type: application/grpc*`）、WebSocket 等 `Upgrade` 请求——走**双工调用**：上游通道为 rpc:2 时请求体按 §2.6.2 边收边发、应答可提前开始（gRPC 的 bidi 流因此可用），HTTP 请求 trailer 随终止块带过去；上游只有 rpc:1 时请求体在网关缓冲（上限 64 MiB，超出回 413），升级请求回 501。
- HTTP/1.1 `Upgrade`（如 WebSocket）：上游回 101 后网关向客户端回 101，并把升级后的连接按 §2.6.5 隧道化。

响应：
- RPY → 状态码取 `Rpc-Status`，头与 body 原样；不超过 256 KiB 的整体缓冲后带 `Content-Length` 发出。
- ANS 流 → **立即**逐块转发（HTTP/1.1 分块 / HTTP/2 DATA 帧），不做缓冲（SSE、gRPC 流依赖这一点）；`Rpc-Trailers` 应答 trailer 转成 HTTP trailer（HTTP/2 的结尾 HEADERS 帧）。
- ERR → 映射状态码（§2.4），附 `X-Beep-Error-Code: NNN`，body：`{"error":{"code":NNN,"message":"..."}}`。

因此任何语言的 gRPC 客户端（明文 h2c）指向网关即可调用 BEEP 网络另一侧的 gRPC 服务（经 §3.4 的 `h2c://` 后端）。

保留端点：

| 端点 | 说明 |
|---|---|
| `GET /__beep/health` | 各上游会话状态（JSON） |
| `GET /__beep/stats` | 计数器（JSON） |
| `GET /__beep/metrics` | Prometheus 文本格式 |
| `POST /__beep/raw` | 原始 BEEP 消息透传，见 §3.3 |

### 3.2 TCP JSON-Lines（出向，给没有 HTTP 库的语言：COBOL、Fortran、嵌入式 C……）

每行一个 JSON 对象（UTF-8，`\n` 结尾）。一条连接上可并发多个请求，按 `id` 匹配，响应可乱序。

请求：

```json
{"id": 1, "method": "POST", "target": "/echo",
 "headers": {"Content-Type": "text/plain"},
 "body": "hello",
 "upstream": "default", "timeout_ms": 5000}
```

- `headers` 可为对象或 `[[name, value], ...]` 数组。
- 二进制 body 用 `"body_b64"` 代替 `"body"`。

响应：

```json
{"id": 1, "status": 200, "headers": [["Content-Type", "text/plain"]],
 "body": "hello", "body_b64": "aGVsbG8="}
```

- `body` 仅当响应体是合法 UTF-8 时出现；`body_b64` 总是出现。
- 失败：`{"id": 1, "error": {"code": 550, "message": "..."}}`。

### 3.3 原始 BEEP 消息透传（`POST /__beep/raw`）

供需要调用**其它 profile**（非 RPC profile）的应用使用：

- 请求头 `X-Beep-Profile: <profile uri>`（必需）、可选 `X-Beep-Upstream`。
- 请求体 = MSG 载荷原文（完整 MIME 实体，含头部和空行）。
- 响应 `200`，头 `X-Beep-Reply: RPY|ERR|ANS`：
  - RPY/ERR：body = 应答载荷原文，`Content-Type: application/octet-stream`；
  - ANS：`Content-Type: multipart/mixed; boundary=...`，每个 part 恰好是一条 ANS 的载荷原文（按 ansno 顺序）。

### 3.4 入向：BEEP → HTTP 后端

网关监听 BEEP 端口并在 greeting 中通告 rpc:1 与 rpc:2。每个 RPC MSG 被转换为对配置后端的 HTTP 请求。后端地址：

| 形式 | 含义 |
|---|---|
| `http://host:port[/base]` | HTTP/1.1 明文 |
| `h2c://host:port` | HTTP/2 明文（prior knowledge），用于 gRPC 等只说 HTTP/2 的后端；不支持升级 |
| `https://host[:port][/base]` | TLS，ALPN 协商 HTTP/2 或 HTTP/1.1；默认信任 Mozilla 根证书，`backend_ca_file` / `--backend-ca` 指定自有 CA |

- `Rpc-Method` + `Rpc-Target` → 请求行；`Rpc-Authority` → `Host`（HTTP/2 后端用 `:authority`）；其余头透传（含 `TE: trailers`）；`Rpc-Timeout` → 等待后端响应头的超时。
- 分块请求体（§2.6.2）边收边转发给后端，请求 trailer 转为 HTTP 请求 trailer；请求方中止 → 后端请求被中断。
- 后端响应没有 body（含 gRPC trailers-only）→ 空 body 的 RPY。
- 后端响应带 `Content-Length` 且不超过 `rpy_max_bytes`（默认 8 MiB）→ 单个 RPY（若后端还带 trailer，则改用 ANS 流以携带 trailer）。
- 否则（分块、`text/event-stream`、gRPC、超大）→ ANS 流，后端的 HTTP trailer 变成 `Rpc-Trailers` ANS，最后 NUL。
- `Rpc-Upgrade`：向后端发 `Connection: upgrade` + `Upgrade: …`；后端回 101 后按 §2.6.5 隧道化。
- 后端不可达 → `ERR 421`；超时 → `ERR 451`；其它失败 → `ERR 554`（消息里带完整错误链，如 TLS 证书校验失败的原因）。

于是任何语言写的现成 HTTP 服务（Express、Spring、Django、Gin、Rails、Laravel、ASP.NET……）都能零改动挂到 BEEP 网络上。
