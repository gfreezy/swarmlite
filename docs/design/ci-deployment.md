# CI 通过 HTTP 部署：实现方案

状态：第 1–11 节记录 v0.1.47 已实现的 HTTP 基础方案；使用方法见 [HTTP(S) CI deployment](../ci-deployment.md)。已覆盖 workspace 单元测试和 Linux Docker + 独立 Caddy 的 HTTP 部署、原地 restart、Controller 重启恢复 E2E；尚未在真实 Podman daemon 上验证。第 12 节的可选域名 HTTPS 扩展已实现，入口策略和 URL 输出规则覆盖前文对应规则；公网多 Gateway 的公共 CA 签发和续期仍需环境联调。

## 1. 目标与范围

CI 推送镜像后，向 Gateway 的 HTTP 80 端口发送一个 POST 请求，由 Gateway 转发到 Controller 的部署端口，触发更新指定 Service。第一版复用现有 Gateway，不要求公网域名或 TLS 证书；Controller 无需同机运行 Gateway。CI 不安装、不调用 Swarmlite CLI；部署入口仅提供 HTTP API，不增加 CLI 部署支持。Controller 使用已经保存的应用配置，不要求 CI 持有 Stack 文件。

第一版采用独立部署 Token；不引入 GitHub App、OIDC、任意脚本 Hook、完整 Stack 上传或 CI 自动回滚。管理员仍可通过现有可信连接管理凭据和执行管理操作；这些 CLI 能力不作为 CI 部署入口。

首次创建应用仍使用 Stack。CI 只能刷新已有 Service 已配置的镜像引用，不能修改 image reference、environment、volume、config、route、replicas 或 registry credentials。

## 2. 用户接口

管理者通过现有可信连接创建凭据（以下命令仅用于管理准备，不在 CI 中执行）：

```bash
swarmlite deploy-token create \
  --name github-production-api \
  --service production.api \
  --expires-in 90d

# 永久有效，仍可随时撤销
swarmlite deploy-token create \
  --name github-production-api-permanent \
  --service production.api \
  --expires-in never

swarmlite deploy-token list
swarmlite deploy-token list --show-secret
swarmlite deploy-token revoke <token-id>
```

`--expires-in` 默认 `90d`，接受正数时长（如 `24h`、`90d`）或显式的 `never`。`never` 表示不自动过期，不表示不可撤销；不使用 `0` 或极大的时间戳代表永久。永久 Token 与有限期 Token 使用相同的 Service 授权和查看规则，未撤销时可通过 `list --show-secret` 再次查看。创建永久 Token 时显示明确的 `Token never expires; revoke or rotate it manually.` 提示；HTTP 明文传输的风险不因永久有效而改变。

每个 Token 第一版只绑定一个确切的 Service ID，不绑定 image reference。管理者更新该 Service 的镜像引用后，原 Token 仍可刷新其当前配置的镜像，无需重新发放凭据。CI 仍不能通过部署接口自行修改镜像引用。

创建时显示完整 Token；默认 list 只显示元数据，不显示密钥或摘要。管理员可通过 `list --show-secret` 再次查看有效且未撤销的完整 Token，部署 Token 本身无此权限。轮换采用“创建新 Token、更新 CI Secret、撤销旧 Token”。

创建和 list 均输出对应 Service 的部署 URL 模板：`http://<GATEWAY_HOST>/v1/deploy/services/{service_id}/redeploy`。其中 Service ID 已按 URL 路径编码并填入，`<GATEWAY_HOST>` 始终保留为占位符，由使用者替换为 Gateway 的实际公网 IP 或域名，可包含自定义端口；默认 HTTP 80 无需填写端口。替换完成后再将完整 URL 放入 CI 的 `DEPLOY_URL` 配置。不新增公网入口配置，也不自动探测或推断 Gateway 公网 IP。

创建和 `list --show-secret` 额外显示完整 Token，以及包含 Authorization 头、幂等键、空 JSON body 和相同地址占位符的 curl 模板。仅地址是占位符，不改变完整 Token 的显示权限；URL 不包含 Token。使用者替换占位符后即可复制执行，原样保留占位符不能触发部署；仅有 URL 不能完成鉴权，浏览器 GET 也不会触发部署。

`list --show-secret` 的单条输出示意（实际输出仍保留地址占位符；Token 和请求键在此仅为示例）：

```text
SERVICE     production.api
STATUS      active
EXPIRES     never (permanent)
DEPLOY_URL  http://<GATEWAY_HOST>/v1/deploy/services/production.api/redeploy
TOKEN       sldt_demo.example-secret
CURL        curl --fail-with-body --silent --show-error --request POST --header 'Authorization: Bearer sldt_demo.example-secret' --header 'Idempotency-Key: manual:550e8400-e29b-41d4-a716-446655440000' --header 'Content-Type: application/json' --data '{}' 'http://<GATEWAY_HOST>/v1/deploy/services/production.api/redeploy'
```

CLI 每次生成 curl 示例时创建一个新的随机请求键；复制同一条命令重试会复用原键，不重复发布。新的独立发布需重新生成命令或替换请求键。CI 自动化仍使用下述由 workflow 标识构成的稳定键，而不反复生成随机键。

CLI 的错误、日志、提示和状态文案统一使用英文。配色复用现有 `ansi()`、表格渲染和全局 `--color auto|always|never`，不新增独立配色参数或依赖：

- 表头和字段名加粗，Service 名使用青色，Token ID 等次要元数据使用暗色；有限有效期显示明确的 UTC 到期时间，永久显示 `never (permanent)`。
- `active` 使用绿色，`expired` / `revoked` 使用红色；`never (permanent)`、地址占位符 `<GATEWAY_HOST>` 和警告使用黄色。状态和风险同时用文字说明，不只靠颜色区分。
- 完整 Token 值和 curl 命令保持纯文本，不插入 ANSI 控制码；可为它们的字段名着色。curl 内的地址占位符也保持纯文本，避免干扰复制和替换。
- 默认 `auto` 分别按 stdout/stderr 是否为终端决定颜色；管道、重定向、`TERM=dumb` 或设置 `NO_COLOR` 时不着色。沿用 `SWARMLITE_COLOR`，显式 `--color always` 强制着色、`--color never` 完全关闭；彩色与无色输出的字段、文字和表格对齐保持一致。HTTP API 不包含 ANSI 控制码。

CI 仅需发送以下请求，HTTP 客户端可使用 curl 或 CI 平台原生的 HTTP 请求能力：

```bash
set -euo pipefail
# DEPLOY_URL 使用替换地址占位符后的完整 URL，DEPLOY_TOKEN 由 CI Secret 注入
http_status=$(curl --fail-with-body --silent --show-error \
  --proto '=http,https' --connect-timeout 10 --max-time 30 \
  --output deploy-response.json --write-out '%{http_code}' \
  --request POST \
  --header "Authorization: Bearer $DEPLOY_TOKEN" \
  --header "Idempotency-Key: github:$GITHUB_REPOSITORY:$GITHUB_RUN_ID:$GITHUB_RUN_ATTEMPT" \
  --header 'Content-Type: application/json' \
  --data '{}' \
  "$DEPLOY_URL")
test "$http_status" = 202
cat deploy-response.json
```

Controller 持久化接受请求后立即返回 `202 Accepted` 和 operation ID，不在触发请求中等待拉取或部署健康。一个 POST 即可完成触发；CI 必须检查响应码恰为 `202`，不能只依赖 HTTP 客户端的退出码。`202` 只代表已受理，不代表部署成功。HTTP 客户端超时或退出不取消服务器已接受的操作。

若需要观察部署结果，可额外查询状态接口；这不是触发部署的必需步骤，也不需要 CLI。断线后使用同一 operation ID 恢复查询，不重新提交部署：

```http
GET /v1/deploy/operations/{operation_id}
Authorization: Bearer sldt_...
```

## 3. 明确区分两种 restart

| 行为 | `service restart` | `service restart --pull` |
| --- | --- | --- |
| 执行动作 | 对现有容器执行 restart | 拉取后滚动重建 |
| Container ID / Task ID | 保留 | 替换 |
| image ID | 保留原容器使用的 ID | 使用本次解析的节点本地 image ID |
| 拉取镜像 | 不执行 | 显式执行，不受 Stack pull_policy 阻止 |
| Service revision | 不改变 | 增加一次，即使镜像内容相同 |
| Stack 配置 | 不改变 | 不改变，包括持久化 pull_policy |
| CLI 身份 | 仅 Admin | 仅 Admin |
| CI 部署 Token | 不允许普通 restart | 仅通过 HTTP redeploy API 刷新绑定 Service |

表中的命令均为管理员操作。CI 的 HTTP redeploy 与管理员 `service restart --pull` 共用服务端拉取和滚动更新逻辑，但不提供部署 Token 的 CLI 调用方式。

普通 restart 遵循容器已有的 stop signal 和 grace period，保留容器创建配置及可写层。它不调用 create/remove；缺少目标容器时报告失败。独立的集群故障恢复仍有自身的调度行为，不能把它算成 restart 的隐式成功。

普通 restart 默认每次处理一个副本，并等待该副本重新健康再处理下一个；单副本有服务中断。第一版不增加可调并行度。

两种操作都拒绝 Job，零副本或没有可执行目标时返回明确错误。

## 4. 网络入口和鉴权

主入口链路为 `CI → Gateway:80 → Controller 内网地址:17082`。同一 Controller 进程提供两套 router：

| 入口 | 用途 | 接受的凭据 |
| --- | --- | --- |
| 原管理监听地址，例如内网 `17080` | 现有管理 API；Token 管理；新操作接口 | 原 cluster token；部署接口另外接受部署 Token |
| 默认启用的部署监听地址 `0.0.0.0:17082` | 只挂载 `/v1/deploy/*`，作为 Gateway 的固定上游 | 仅部署 Token |

新增 Controller 启动配置 `--deploy-listen`，默认值为 `0.0.0.0:17082`，Controller 启动时默认启用部署监听，绑定所有 IPv4 网卡；配置通过 node composition root 传给 Controller。默认使用 Gateway 的 80 端口对外，不把 Controller 改为监听 80，也不要求 Controller 运行 Gateway。

Controller 在 Gateway 配置生成逻辑中新增系统级部署路由，随现有 Gateway assignment/generation 机制下发到已启用的 Gateway，不手工修改托管 Caddy 配置：

- 在 HTTP 80 入口保留 `/v1/deploy/` 路径前缀，不要求 Host 匹配域名，允许使用 Gateway 公网 IP。
- 路由排在应用路由之前且为 terminal；该路径下的请求不回退到业务应用，也不能被应用 catch-all 或自动 HTTPS 重定向截获。其他业务路由和 HTTPS 行为保持不变，不全局关闭自动 HTTPS。
- 仅转发该前缀到 Controller 的部署 router，不代理整个 `/v1/*`，也不连接管理端口 `17080`。上游由 Controller 的集群可达通告地址和部署监听端口生成；自定义监听时必须保证通告地址可达。不能使用 `0.0.0.0`、远程 Gateway 的回环地址或 Gateway 的公网入口作为上游，避免错误寻址和代理循环。
- 原样保留请求 method、path、body、Authorization、Idempotency-Key 和 Content-Type，透传响应状态、body 与相对 Location。Gateway 不追加 cluster token，不代替 Controller 鉴权，不缓存部署响应，也不主动重试 POST；响应丢失后由调用方使用原幂等键恢复。
- 系统路由不属于业务 Stack，不依赖目标 Service 的任务或健康状态，不随业务部署、缩容或删除消失。Gateway 未运行或上游不可达时请求失败，不隐式改用管理 API。

Gateway 保持既有监听配置，默认包含 80；自定义配置未监听 80 时，管理员需显式启用相应入口并放行防火墙。Controller 不为此自动创建 Gateway、抢占 80 或覆盖用户已有监听设置。

Controller 不需要知道 Gateway 的公网 IP，不新增对外基地址配置、Token URL 存储字段或公网地址发现逻辑。URL/curl 模板仅由管理员 CLI 根据 Token 绑定的 Service ID 渲染，地址固定显示 `<GATEWAY_HOST>`；不从节点列表、请求的 Host / X-Forwarded-Host 或管理员 CLI 的 SSH/本地连接地址推断。实际访问地址由使用者在脚本或 CI 中填写，不参与 Token 的授权绑定；公网地址变化后，原 Token 仍有效，只需更新 CI 使用的 URL。Gateway 到 Controller 的内部上游配置与该占位符无关。

默认 Gateway 模式下，部署者通过防火墙或安全组将 Controller 的 `17082` 限制为 Gateway 节点可达，公网只需开放 Gateway 的 80；有固定 CI 出口时可进一步限制来源。监听 `0.0.0.0` 本身不提供网络隔离。HTTP 不加密 Token、请求和响应，经 Gateway 转发也不会增加传输加密，公网使用仍存在凭据被截获、重放和请求被篡改的风险；Service 限权、过期、撤销、限频和幂等不能消除链路风险。第一版明确接受这一限制，不把 HTTP + Token 描述为安全的公网传输。

无 Gateway 的备用方式是显式直连 Controller 部署端口，由使用者在 CI 中填写对应 Controller URL；也可在端口空闲且具备绑定权限时，显式配置 `--deploy-listen 0.0.0.0:80`。这不是默认入口，不会自动切换或与已有 Gateway 共用同机同地址的 80 端口。采用直连时，部署者需另外放行该端口并承担 HTTP 公网暴露风险。

原管理端口保留集群内网通信，并通过防火墙阻止公网访问。公网部署 router 不挂载 `/v1/stacks`、`/v1/configs`、`/v1/nodes`、`/v1/kv`、日志或 registry proxy，也不接受 cluster token。

不要把部署 Token 加进现有 `authorized()` 的布尔 OR 判断。新增明确身份：

```rust
enum Principal {
    Admin,
    Deploy { token_id: String, service_id: String },
}
```

原 API 继续要求 Admin。部署 API 先验证身份；提交时精确比较 Service ID，查询时检查操作归属。状态响应只包含本次操作及目标 Service 的进度，不返回完整 Stack、其他服务配置或原始敏感错误。

Token 格式建议为 `sldt_<id>.<secret>`，secret 使用至少 32 字节安全随机数。为支持管理员再次查看，服务端同时保存 secret 明文和 SHA-256 摘要，鉴权使用摘要并常量时间比较。Token 记录 cluster ID、名称、Service ID、创建/过期/撤销时间；`expires_at_unix_ms` 可空，`null` 表示永久。有效性要求未撤销，且过期时间为空或当前时间严格早于过期时间；到期时刻即失效。撤销时清除 secret 明文，保留摘要和审计元数据。

第一版不引入独立加密密钥管理；Controller 数据目录、SQLite 文件及备份均按凭据保护，限制为运行用户可读写。数据库或备份泄露会暴露其中仍保存的 Token，摘要不能消除这一风险。

第一版通过 Gateway 的 HTTP 系统路由对外，不强制 HTTPS，也不引入证书签发或公网域名作为前置条件。后续使用 HTTPS 入口时，更新 CI 使用的 URL 与代理配置即可，Controller 部署协议不变；需要避免反向代理链路暴露明文凭据。CI 示例只允许 HTTP(S)，不自动跟随重定向；使用 HTTPS 时正常验证证书，不使用 `--insecure`。不依赖可伪造的 `X-Forwarded-Proto` 判断身份。

调用部署 API 时，凭据只放在 Authorization 头中。除管理员创建或显式查看 Token 的响应及 CLI 输出外，不返回密钥；CLI/代理/Controller 的诊断日志不记录密钥。提交 body 限制为 16 KiB，长轮询最多 25 秒；按凭据限制新操作频率、按 Stack 限制并发。过期或撤销后拒绝新的 API 请求。撤销不回滚或取消已经接受的部署。

## 5. API 协议

管理入口独有：

```text
POST   /v1/deploy-tokens
GET    /v1/deploy-tokens
DELETE /v1/deploy-tokens/{id}
POST   /v1/services/{id}/restart
```

创建 Token 的 `expires_in` 为字符串，省略时使用 `"90d"`，传 `"never"` 创建永久 Token；不接受显式 `null`、零、负数、无效或溢出的时长，这些情况返回 `400`。Controller 根据接收时间计算有限期 Token 的到期时间；创建和列表响应中的 `expires_at_unix_ms` 为可空的 UTC Unix 毫秒时间戳，永久时明确返回 `null`，不省略字段。CLI 将 `null` 渲染为 `never (permanent)`，不显示为未知或已过期。

Token 创建和列表 API 不返回推断的公网 URL，也不新增 `deploy_url` 字段；管理员 CLI 使用已有 Service ID 元数据生成带 `<GATEWAY_HOST>` 占位符的 URL 模板。`GET /v1/deploy-tokens` 默认仅返回元数据；显式传入 `show_secret=true` 时，才额外返回有效且未撤销的完整 Token，对应 CLI 的 `list --show-secret`。CLI 在可显示完整 Token 时渲染带相同地址占位符的 curl 模板，不在 URL 路径或 query 中嵌入密钥。Token 管理接口仅允许 Admin；创建和显式查看密钥的响应设置 `Cache-Control: no-store`，审计仅记录查看者、Token ID 和时间，不记录密钥或响应体。

两套 router 的部署操作接口具有相同协议：

```text
POST /v1/deploy/services/{id}/redeploy
GET  /v1/deploy/operations/{id}
```

管理入口额外挂载 `GET /v1/operations/{id}`，供 Admin 观察包括普通 restart 在内的操作。

`redeploy` 固定表示 pull + rolling replacement。路径中的 Service 使用确切的规范化 ID；body 可为 `{}`，`source` 是可选审计元数据；使用 deny_unknown_fields，不接受 Stack YAML、image、command 或任意 URL。`Idempotency-Key` 必填，缺少或格式无效时返回 `400`：

```http
POST /v1/deploy/services/production.api/redeploy
Authorization: Bearer sldt_...
Idempotency-Key: github:acme/api:123456:1
Content-Type: application/json

{"source":{"repository":"acme/api","sha":"abc123","run_id":"123456"}}
```

成功接受时返回：

```json
{
  "operation_id": "op_...",
  "service": "production.api",
  "kind": "pull_redeploy",
  "status": "queued",
  "deployment_generation": null
}
```

POST 必须在持久化成功后才返回 `202`，并携带指向 `/v1/deploy/operations/{operation_id}` 的 `Location` 响应头。generation 在关联部署创建后出现，不能要求调用方提前查询整个 Stack。

状态接口支持 `after_revision`、`wait_seconds` 的有界长轮询，并返回 operation phase、目标 Service 的进度、脱敏错误和可选 deployment generation。普通 restart 没有 deployment generation。

operation status 使用 `queued / running / succeeded / failed`，phase 区分 pulling、restarting、waiting_health 等进度。`succeeded` 必须代表目标重新健康；关联 deployment 的 stalled/blocked 作为独立字段暴露，不冒充成功或已经不可恢复的失败。可选的 CI 状态检查根据响应判断部署是否成功；管理员修复 stalled/blocked 后，仍查询原 operation，无需再次触发。

错误包括：`400` 请求参数或幂等键无效；`401` 凭据无效；`403` 身份/目标不允许；`409` 操作冲突或请求键冲突；`429` 超频；`503` 存储暂不可用。跨 Token 的 operation 查询返回不泄露存在性的 `404`。

普通 Token 请求中的 repository、sha、run_id 是客户端声明的审计信息，不用于证明 GitHub 身份，也不根据它们直接授予分支权限。

## 6. 持久化、幂等与并发

Controller SQLite 新增：

- `deploy_tokens`：cluster ID、token ID、secret 明文、摘要、固定授权目标及可空的 `expires_at_unix_ms`；数据库 `NULL` 表示永久，secret 在撤销时置空。
- `service_operations`：operation ID、请求主体身份、Service/Stack、kind、request key/hash、phase、目标 revision、deployment generation、审计元数据与时间。普通 restart 的任务快照、node/container/image ID、执行阶段、截止时间和结果，以及 pull redeploy 的各节点准备结果，存入该行的结构化 operation 文档，不再拆分独立的 `service_operation_tasks` 表；与期望状态在同一事务内提交。

Agent SQLite 新增 `service_operation_attempts`，保存 `(cluster, operation, task, container)` 的执行声明与结果。不要复用一次性 Job 的 start-attempt ledger。

幂等键按 `(cluster_id, principal_id, idempotency_key)` 唯一。相同键和规范化请求返回原 operation；不同请求返回 `409`。检查顺序为认证/授权、已有请求查询、仅对新操作执行频率与并发限制，避免正常重试被活动操作拒绝。接收事务内再次验证 Token 未撤销且未到期（永久 Token 无到期限制）和目标 revision，避免校验与受理之间的竞态。

接收 operation 和占用 Stack 的持久化操作槽必须在同一 SQLite 事务完成。建立 deployment 时，更新期望状态、写 operation 与 deployment 的关联也必须同事务完成；需要扩展 repository 的事务入口，不能依次执行两个独立提交。

沿用当前“每个 Stack 一个活动部署”的模型。同一 Stack 的 restart、pull redeploy、deploy、scale、rm、retry、rollback 共用冲突检查。管理员 retry 本 operation 关联的 deployment 时继续原操作，不被自身锁阻止；管理员显式 supersede 时，旧 operation 标记 failed/superseded。第一版不让 CI 自动 replace 活动部署。不同 Stack 可并行。

operation 受理后的执行由 Controller 后台工作循环驱动，不绑定 HTTP handler 生命周期。Controller 重启后从 SQLite 恢复，不重复创建 generation。已撤销凭据的后台操作仍按原接受记录推进，但该凭据不能继续查询。

终态 operation 与幂等记录至少保留 90 天；未完成记录不清理，超过保留期后同一请求键不再保证去重。保留期内的记录清理必须同时考虑 operation 和请求键，不能只保留成功响应而丢掉正在执行的事实。

## 7. 普通 restart 的执行

```text
持久化任务快照 → 标记目标正在重启 → Agent 核对身份
→ 重启原容器 → 确认实际启动 → 等待健康 → 下一个副本
```

快照至少包含 Service revision、Task ID、node ID、Container ID、原 image ID。补齐 Controller 的 observed image ID 记录；当前 TaskReport 虽上报 image ID，TaskRecord 尚未保存它。Agent 执行前复核归属和时效；不使用容器名称模糊匹配，不向其他容器重定向操作。

Runtime trait 新增 `restart_task`，调用 Docker/Podman 的容器 restart 能力，使用容器既有停止设置。Agent 将本任务的普通 reconciliation 与 restart 串行执行；Controller scheduler/heartbeat 在有界 restart 窗口内不把预期停止误判为故障替换，但仍对真实节点失联、容器消失和超时作出处理。重启期间目标从可用 upstream 中移出，恢复健康后再加入；第一版不承诺现有连接无损排空。

现有 AgentCommandBroker 是内存队列，不能作为持久化操作的唯一事实来源。通过 heartbeat 增加持久化 restart assignment/result，由 Agent 本地 ledger 去重。

Docker restart 无法与本地 SQLite 做原子提交，不能承诺严格 exactly-once。执行前记录原 StartedAt；若 Agent 在调用后崩溃，恢复时结合 Container ID、StartedAt 和健康状态确认结果。无法确认时标记 `failed`，错误为 `restart_outcome_unknown`，不自动再次发送 restart。管理员可以先检查，再显式提交一个新操作。

## 8. HTTP redeploy / 管理员 `--pull` 的执行

```text
记录配置镜像引用 → 准备目标节点镜像 → 固定本次解析结果
→ 创建 deployment 并增加目标 revision → 现有滚动更新 → 等待健康
```

复用现有 current_stack、镜像解析、滚动调度、健康检查和 Gateway 收敛。增加明确的目标策略，区别于当前 Force（立即 bump）与 DetectChanges（镜像未变就不 bump）：目标 Service 先解析镜像，然后无论内容是否变化都只 bump 一次；其他 Service 不刷新镜像，也不更改 revision。

本次 pull 覆盖持久化 pull_policy 的执行选择，但不改 ServiceSpec 或该字段。所有准备目标成功前不启动替换；部分节点拉取失败时保留旧任务。扩展/迁移到新的候选节点时，必须先补齐该节点的镜像准备，不能回退到隐式拉取可变 tag。

TaskAssignment 增加明确的 prepared image ID，而不是只传 image_resolved 布尔值。Runtime 使用本次 operation/deployment 保存的该节点 image ID 创建容器；ID 缺失或被清理时暂停/报错，不重新按 tag 拉取其他内容。解析结果要持久化，不能仅依赖 Agent 的 prepared_images 内存 map。

image ID 是节点 Runtime 的标识，不把它当成可跨节点拉取的 registry manifest digest；多架构节点可以有不同 image ID。审计记录请求的 image reference 和各节点 image ID，registry 能提供时另外记录 manifest digest。

固定可变 tag 的模式仍受镜像仓库更新影响，不承诺跨节点拿到同一时刻版本，也不宣称现有 Stack snapshot 能回滚旧镜像字节。第一版不自动回滚；需要精确版本部署/回滚时，另行扩展 digest 更新协议。

## 9. HTTP 调用、兼容与 CI 配置

CI 只持有部署 URL 和部署 Token，直接调用部署 API；不依赖 SWARMLITE_CONTROLLER/SWARMLITE_TOKEN、Swarmlite CLI 安装或 CLI/Controller 版本匹配，也不调用 status、bootstrap、inspect 或 Stack 查询。需要进度时从 operation HTTP 接口获得授权范围内的数据。

CI 为每次逻辑发布生成 `Idempotency-Key`，网络重试复用同一值及请求体；示例使用 repository、run ID 和 run attempt。响应丢失时使用原键重试 POST，取回同一个 operation；不要生成新键来恢复观察。新 run attempt 使用新键，表示一次新的显式发布尝试，不与上一次尝试去重。保存响应中的 operation ID，供可选的状态查询或人工排查使用。服务端仍在 reconciling/stalled/blocked 时，不把 HTTP 客户端超时写成部署 failed。

CLI 仅增加管理员 Token 管理和 restart 语义调整，不支持以部署 Token 调用部署入口，也不新增 CI deploy/status/attach 命令。旧 `/force-update` 保持 Admin-only 的原有行为，兼容旧客户端；管理员的新 CLI 普通 restart 使用新 restart API，--pull 使用管理监听入口的 redeploy API。新 CLI 遇到旧 Controller 明确报不支持，不静默降级到旧语义。CI 调用不支持部署 API 的旧 Controller 时明确失败，不回退到管理 API。

新增 Agent capability，Controller 遇到不支持 restart assignment 或 prepared image ID 的 Agent 时拒绝相应操作，不能忽略字段后继续执行。

新增持久化操作状态需要版本化 schema 迁移：对当前 schema 12 提供事务迁移和升级前备份，保留 jobs ledger、KV、configs、registry credentials 和 deployment history；备份使用 SQLite 支持的一致性备份方法，包含 WAL 中尚未 checkpoint 的数据。旧 Controller 必须拒绝新版存储，不能忽略新增操作数据后覆盖数据库。原本不支持的更老版本不借此自动接纳。

CI workflow 使用部署 Environment；使用者将 deploy-token 输出中的 `<GATEWAY_HOST>` 替换为实际公网地址，必要时修改端口或 scheme，再将完整 URL 放入 `DEPLOY_URL` 环境变量。Token 放入 `DEPLOY_TOKEN` Secret，不安装 Swarmlite CLI，也不需要知道 Controller 内网地址。构建阶段可推送 commit tag，串行发布 job 内再更新 prod tag、发送 HTTP POST；生产 concurrency 使用 cancel-in-progress: false。提升 prod tag 前确认本次 commit 仍是允许发布的分支最新提交，过时构建跳过发布，避免旧构建晚完成后覆盖新版本。不要让未经信任的 PR 代码在可读取部署 Secret 的 job 中执行。

默认 job 只确认触发请求已受理，不宣称部署健康。若需要由 CI 确认发布成功，额外轮询 operation，直到 `succeeded`、`failed` 或达到 CI 自己的等待上限。仅触发时，CI concurrency 不能覆盖后台部署的完整生命周期，服务端 Stack 操作槽仍负责防止并发部署；活动部署期间的新触发返回 `409`，不自动替换或暗中排队。

Token 轮换、固定 tag 发布顺序和凭据撤销分别解决不同问题：幂等不是防止泄露 Token 发起新请求的机制；客户端 source 字段也不是可信来源证明。能发布镜像的主体拥有该应用运行权限，应用已有挂载与凭据决定其实际影响范围。

## 10. 代码改动范围

| 位置 | 改动 |
| --- | --- |
| `swarmlite-core/src/model.rs`、`config.rs` | Token/operation 协议、Agent capability、任务 observed/prepared image ID、部署监听配置；不新增公网入口配置或 URL 字段 |
| `swarmlite-core/src/gateway.rs`，Controller 的 `gateway_control` | HTTP 部署系统路由、固定 Controller 部署上游、路由优先级及 Gateway 配置下发；不依赖 Controller 同机运行 Gateway |
| `swarmlite-node/src/lib.rs` | 配置传递、两个监听入口的启动及关闭 |
| `swarmlite-controller/src/controller/api.rs`，新增 auth/deploy_api/operations 模块 | 保持管理鉴权边界；部署身份、Token 管理、受限 router 和 operation 后台推进 |
| `swarmlite-controller/src/storage.rs` | 新表、迁移、受理/占用操作槽/建立部署关联的原子事务 |
| Controller 的 deployment/heartbeat/lifecycle 和 scheduler | 先准备镜像后强制滚动、镜像 ID 下发、restart 窗口、恢复与统一冲突检查 |
| `swarmlite-agent/src/lib.rs` | restart assignment/result、本地去重、与普通 reconciliation 协调 |
| `swarmlite-platform/src/runtime.rs`、`local_state.rs` | 原容器 restart、按准备好的 image ID 创建、执行声明/结果与 StartedAt 恢复证据 |
| `swarmlite-cli/src/lib.rs`、`cluster_cli.rs`，`swarmlite-client` | 仅管理员 Token 管理、有限/永久有效期、带地址占位符的 URL/curl 模板与语义配色、restart 参数和操作模型适配；不增加部署 Token 的 CLI 支持 |
| CLI UI commands、相关文档与 E2E | UI 跟随新 restart 语义；直接 HTTP 调用的 CI 示例；故障/越权与恢复验收 |

## 11. 分阶段实施与验收

### A. 普通 restart 语义

增加 operation 模型、持久化、Agent assignment/ledger、Runtime restart 和 scheduler 协调；CLI 普通 restart 改走新 API。

验收：Container/Task/image ID 保留，revision 不变，Registry 无网络请求，可写层保留，多副本依次恢复健康；任务停机期间不意外创建替代容器；断线和 Agent/Controller 重启后的结果不产生无意重复 restart。

### B. 显式 pull redeploy

增加管理员 --pull 与 HTTP redeploy 共用的部署策略：先准备镜像再强制滚动，并下发 prepared image ID。

验收：always/missing/never 均能显式刷新，同镜像也重建；配置与其他 Service 不变；拉取失败不停止旧任务；准备后 tag 被覆盖时仍使用准备好的 ID；新调度节点先准备；Controller 重启不重复 bump revision。

### C. HTTP 部署入口与凭据

增加支持有限/永久有效期的 Token 管理、默认启用的 Controller 部署 router/listener、Gateway HTTP 80 系统路由及配置下发、带地址占位符的 URL/curl 模板输出和 CLI 语义配色、授权、脱敏状态响应和限频；默认经 Gateway 转发，不依赖域名、证书或公网地址发现。

验收：Controller 不运行 Gateway 时，CI 仍可经远程 Gateway 的公网 IP 和 80 端口触发，并经同一入口查询 operation；请求不被业务 catch-all 或 HTTPS 重定向截获，其他业务路由和 HTTPS 行为不变；Gateway 不代理管理 API、不注入 cluster token，部署路径错误不回退到业务应用；URL、Token、幂等键及响应正确透传；显式直连备用方式可用且不会抢占已有 Gateway 的 80。创建和列表的 URL 模板始终使用 `<GATEWAY_HOST>`，仅填入正确编码的目标 Service ID，不输出猜测的公网 IP、Controller 内网/管理地址或 SSH 本地隧道地址；显式显示密钥时，替换地址占位符后的 curl 可触发部署，无需任何公网入口配置。允许目标可以发布并观察；管理员修改目标 Service 的镜像引用后，原 Token 仍可刷新当前镜像；其他目标/操作/Token 不可访问；公网入口拒绝 Admin token；过期、撤销和错误密钥均按协议失败；默认列表、URL、部署响应和诊断日志不显示 Secret；仅 Admin 创建或显式查看时返回完整 Token，过期或撤销后不再返回密钥，撤销后存储中的 secret 明文已清除。

有效期与 CLI 验收：省略有效期默认 90 天，`never` 持久化为 `NULL` 且 Controller 重启后仍有效；永久 Token 可查看、使用和撤销，撤销后不能提交或查询操作；有限期 Token 在到期时刻被拒绝，非法时长返回 `400`。验证终端配色、`--color never`、`NO_COLOR`、管道/重定向默认无色及 `--color always` 行为；有色/无色表格对齐一致，永久和地址占位符有文字提示，Token/curl 值不含 ANSI 控制码且替换地址后可直接复制执行。

### D. CI 联调与文档

提供向 Gateway 的 80 端口发送 HTTP POST 的 GitHub Actions 示例，以及 Gateway/Controller 分机配置、Token 创建/查看/轮换、替换 URL/curl 地址占位符、HTTP 明文传输风险、使用原幂等键恢复提交结果、可选的 HTTP 状态查询和故障排查说明；CI 示例不安装或调用 Swarmlite CLI。验收一个 POST 即可触发后台部署，`202` 不被当作健康成功，响应丢失后原键重试不重复发布。检查管理员 UI 生成的 restart 参数与新语义一致，不继续调用旧 force-update。

以 Linux Docker 的真实 Runtime E2E 验证上述身份/镜像/恢复不变量，复用 mock Docker API 做异常边界覆盖，并验证 Podman restart 行为。重点覆盖“提交成功但响应丢失”和“Runtime 完成但 ledger 结果尚未写入”的崩溃窗口，不把普通 happy-path 单测当成幂等证明。

A–D 全部通过后再开放公网部署入口；发布时遵循仓库既有 release workflow。

## 12. 可选域名 HTTPS 扩展

### 配置与两种入口模式

新增集群配置 `gateway.deploy.domain`，由管理员显式填写，沿用现有配置持久化、Gateway assignment/generation 和热更新机制，下发到所有启用的 Gateway：

```bash
swarmlite config set gateway.deploy.domain deploy.example.com
swarmlite config get gateway.deploy.domain
swarmlite config unset gateway.deploy.domain
```

配置值仅接受一个确切的 DNS 主机名，不接受 scheme、端口、路径、IP 或通配符；空值通过 `unset` 表达。不新增 `deploy-advertise-url`，不发现或推断公网 IP。DNS 由使用者管理，可以指向多个 Gateway 或其前置负载均衡入口。

| 集群配置 | Token 创建、列表输出的部署地址 | Gateway 部署接口策略 |
| --- | --- | --- |
| 已设置 `gateway.deploy.domain=deploy.example.com` | `https://deploy.example.com/v1/deploy/services/{service_id}/redeploy` | 仅允许匹配该域名的 HTTPS 443 入口；拒绝 HTTP 部署请求 |
| 未设置域名 | `http://<GATEWAY_HOST>/v1/deploy/services/{service_id}/redeploy` | 保留现有 HTTP 80 入口，地址由使用者替换，不要求 Host 匹配域名 |

这里的“禁用 80”仅指拒绝通过 HTTP 调用 `/v1/deploy` 及其子路径，不关闭 Gateway 整个 80 监听，不影响其他业务 HTTP 路由、证书验证和 Gateway 探测。自定义监听配置仍由管理员负责，不自动覆盖；HTTPS 模式需要提供可达的 443 入口。

设置域名后，HTTP 部署路由在业务路由和自动重定向之前以 terminal 规则返回 `403`，不转发、不触发部署，也不重定向到 HTTPS。所有 Host 下的该 HTTP 路径均拒绝，不能通过 IP 或其他域名绕过。HTTPS 部署路由同时匹配配置域名和部署路径；其他 Host 的部署路径不回退到业务应用。协议判断使用 Gateway 实际连接的 TLS 状态，不信任客户端传入的 `X-Forwarded-Proto`。

复用现有 Caddy 自动证书和证书存储协调能力；证书获取、续期或域名解析失败时，HTTPS 请求失败，不重新开放 HTTP 部署接口。多个 Gateway 的证书验证、签发锁和续期行为必须通过真实联调确认，不能只凭 DNS 配置或共享存储接口宣称已支持。

### Token 与 CLI 输出

Token 仍仅绑定 Service ID，不绑定域名。创建和 `list` 根据命令执行时读取的当前集群配置生成 URL：已设置域名时直接输出真实 HTTPS 地址；未设置时才保留 `<GATEWAY_HOST>`。`list --show-secret` 和创建时输出的 curl 使用同样规则，域名模式的命令无需替换地址即可复制执行。

URL 不存入 Token 记录，Token API 不增加 `deploy_url` 字段；管理员 CLI 通过现有管理 API 读取集群配置，不从 SSH 隧道地址、Controller 地址或请求 Host 推断入口。读取配置失败时明确报错，不把失败当成“未配置域名”而输出 HTTP 地址。

修改或取消域名不会撤销已有 Token；重新查看时显示当前地址，使用者需同步更新 CI 的 `DEPLOY_URL`。域名由旧值变为新值时，旧域名不再作为部署入口；取消域名会恢复 HTTP 模式，配置命令应明确提示明文传输风险。

真实 HTTPS URL、Token 和 curl 命令保持可复制的纯文本值，字段名沿用现有 CLI 配色；未配置域名时保留地址占位符和 HTTP 风险警告。域名模式的 CI 示例使用 HTTPS、正常验证证书，不允许 `--insecure` 或自动降级到 HTTP。

### 网络边界与验收

HTTPS 在 Gateway 终止，Gateway 到 Controller 的现有内网 HTTP 上游和部署协议不变。设置域名不关闭 Controller 的 `17082` 内部监听，也不使其自动具备 TLS；该端口必须继续通过防火墙限制为 Gateway 可达，不能暴露公网。Gateway 拒绝 HTTP 请求不能补救凭据已经被客户端以明文发送的问题，CI 必须直接使用输出的 HTTPS URL。

实现范围为集群配置模型和校验、配置 CLI、Gateway HTTPS/HTTP 拒绝路由及配置下发、Token CLI 读取配置并渲染地址、相关测试和文档；不改变部署 Token 授权、幂等或 operation 协议。

验收至少覆盖：

- 未配置域名时保留现有 HTTP 80 部署和占位符输出；设置域名后，创建和列表均输出正确编码的真实 HTTPS URL，curl 可直接使用。
- 已配置域名时，所有 Host 的 HTTP 部署 POST/GET 均被拒绝，不发生重定向、上游请求或部署；其他业务 HTTP、证书验证与 Gateway 探测不受影响。
- 正确域名可通过 HTTPS 提交和查询；错误域名、IP 或非部署路径不能误入部署或管理接口，证书故障不回退 HTTP。
- 配置变更后所有在线 Gateway 按现有 generation 机制收敛，重新连接的 Gateway 获取当前配置；域名变更、取消与 Controller 重启后 URL 输出和路由策略一致，已有 Token 无需重新创建。下发期间可能存在旧配置节点，切换完成前需确认所有公网 Gateway 已收敛，不能宣称全局原子切换。
- 多 Gateway 场景验证独立节点的首次证书签发、跨节点挑战、共享锁、续期与节点故障；Token CLI 配置读取失败时不输出可能误导的 HTTP URL。
