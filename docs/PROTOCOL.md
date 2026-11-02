# MINTA BEEP Bridge — 协议规范

本文是所有语言实现（Rust 核心、网关、C ABI、各语言原生库、SDK）共同遵守的唯一规范。
分三层：

1. **BEEP 传输层** —— RFC 3080（BEEP Core）+ RFC 3081（BEEP over TCP）的实现子集与约定
2. **RPC Profile** —— 跑在 BEEP 通道上的、承载“后端请求”的自定义 profile：`urn:minta:beep:rpc:1`
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

<greeting><profile uri='urn:minta:beep:rpc:1'/></greeting>
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

### 1.6 默认值

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
- `Host` → `Rpc-Authority`。
- 剥离 `Content-Transfer-Encoding`（BEEP 保留）。
- 客户端发来的 `Rpc-Method` `Rpc-Target` `Rpc-Status` `Rpc-Authority` `Rpc-Reason` 一律剥离由网关重写；允许客户端自带 `Rpc-Id` `Rpc-Timeout`。
- 头值中的 CR/LF 一律剥除。

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

响应：
- RPY → 状态码取 `Rpc-Status`，头与 body 原样。
- ANS 流 → HTTP 分块（chunked）流式输出。
- ERR → 映射状态码（§2.4），附 `X-Beep-Error-Code: NNN`，body：`{"error":{"code":NNN,"message":"..."}}`。

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

网关监听 BEEP 端口并在 greeting 中通告 RPC profile。每个 RPC MSG 被转换为对配置后端（如 `http://127.0.0.1:3000`）的 HTTP 请求：

- `Rpc-Method` + `Rpc-Target` → 请求行；`Rpc-Authority` → `Host`；其余头透传；`Rpc-Timeout` → 请求超时。
- 后端响应带 `Content-Length` 且不超过 `rpy_max_bytes`（默认 8 MiB）→ 单个 RPY。
- 否则（分块、`text/event-stream`、超大）→ ANS 流 + NUL。
- 后端不可达 → `ERR 421`；超时 → `ERR 451`；其它失败 → `ERR 554`。

于是任何语言写的现成 HTTP 服务（Express、Spring、Django、Gin、Rails、Laravel、ASP.NET……）都能零改动挂到 BEEP 网络上。
