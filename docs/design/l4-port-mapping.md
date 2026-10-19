# Swarmlite 原生 TCP 端口映射

状态：v0.1.48 引入；本地验收不代表已部署线上。

采用本仓库自研的最小 Caddy app `swarmlite_tcp`，与现有 HTTP/HTTPS Gateway 共用一个 Caddy 进程。没有引入 caddy-l4、nftables/IPVS 或额外的 L4 容器。保留 Service 的应用健康检查，并增加轻量 TCP connect 探测；不实现 TCP 连接排空，也不承诺 TCP 更新无中断。

## 配置与链路

`tcp_routes` 为可选扩展，不写、写 `[]` 或 `null` 都不启用 TCP；不生成新 Caddy app，不上报空 TCP 状态，HTTP-only 恢复快照仍为 v1。现有 HTTP Stack 无需改配置。

`x-swarmlite`、TCP route 和 TCP backend 的未知扩展字段在新版本中忽略，JSON schema 也允许这些字段；已知字段的类型错误、缺少必填 TCP 字段和非法端口仍报错。其他既有 Service/HTTP 嵌套配置的严格校验不因本功能改变。

```yaml
services:
  motifd-review:
    image: example/motifd-review:latest
    expose: ["8099"]
    deploy:
      replicas: 2
    # 原有 healthcheck 可继续配置，执行和判定逻辑不变。

x-swarmlite:
  tcp_routes:
    - listen: 8099
      backend:
        service: motifd-review
        port: 8099
```

`listen` 是每个已启用 Gateway 的固定 TCP 入口，`backend.port` 是同一 Stack 内的 Service 容器端口，二者可以不同。

```text
客户端 → Gateway:8099 → swarmlite_tcp
       → 任务节点:动态 published port → 容器:8099
```

例如任务节点是 `10.0.0.21`，Docker 将容器的 `8099` 发布为宿主机 `20001`，则 Gateway 连接 `10.0.0.21:20001`。任务不必调度到 Gateway 节点。节点之间必须能够访问这些 published port；本功能不提供 overlay 网络。

Docker 的端口映射只负责最后一段。仅配置业务容器的 `8099:8099` 无法提供跨节点的 Gateway 固定入口。

第一版只提供 `listen`、`backend.service` 和 `backend.port` 三个必填字段。不提供 TLS 终止、SNI 分流、PROXY protocol、UDP、权重、粘性会话或自定义健康探测参数；TLS 可以作为原始 TCP 字节流透传。

## 职责与数据流

1. Stack parser 和 schema 解析、校验 `tcp_routes`。HTTP/TCP 引用端口的并集用于分配任务动态 published port。
2. Controller 从期望 Running、观测 Healthy 的任务中取节点地址及实际 TCP published port，排序去重后写入 Gateway 路由片段。
3. Controller 将 HTTP 配置和 `apps.swarmlite_tcp` 一起通过现有 assignment/heartbeat 下发。不增加独立控制协议或第二套容器生命周期。
4. Node 沿用现有 Caddy `/load`、autosave、恢复快照、证书同步和蓝绿替换流程。TCP 的运行时故障不会使 Caddy app 启动失败。
5. Node 每两秒查询本机 `GET /swarmlite/tcp/status`，通过 `GatewayReport.tcp` 上报监听、后端健康和错误。CLI `gateway status` 和 Web UI 的 Routes 页面分别展示配置应用状态与 TCP 可用性。

只接受观测 Healthy 的任务，因此 Service 已有 healthcheck 继续决定端点是否进入路由；未配置 healthcheck 时仍沿用现有运行状态判定。TCP connect 探测补充判断 Gateway 到 published port 的可达性，不替代应用层健康检查。

## Caddy app

生成的 JSON 示例：

```json
{
  "apps": {
    "swarmlite_tcp": {
      "routes": [
        {
          "id": "demo:8099",
          "listen": ":8099",
          "upstreams": ["10.0.0.21:20001", "[fd00::22]:20004"]
        }
      ]
    }
  }
}
```

无 TCP 路由时不生成这个 app。没有后端时保留 route，`upstreams` 为 `[]`；listener 仍保留，接受后立即关闭新连接。不合成 HTTP 错误页，不等待后端恢复，也不因此重启 Caddy。

每个 TCP route 的 listener 独立维护。使用 Caddy 的可复用 listener 支持现有配置重载及两个 Gateway 进程重叠启动。绑定失败只记录该 route 的错误，并每两秒重试，不向 app `Start()` 返回错误。端口释放后自动恢复监听。接受连接时在健康后端间轮询，单次连接只拨号一次；拨号失败立即将该后端标记不可用，并关闭客户端连接，不重放数据或自动重试该连接。

## 健康检查与资源边界

- Service 原有 healthcheck 保留，决定 Controller 是否发布任务端点。
- Gateway 首次主动探测成功前不向该后端转发。
- 主动 TCP connect 探测间隔两秒、单次超时一秒；连续两次失败剔除，一次成功恢复。
- 实际代理拨号超时三秒；拨号失败立即剔除，后续主动探测可恢复。
- 探测只建立并关闭 TCP 连接，不发送业务请求。可能产生后端的空连接日志，不能证明应用协议可用。
- 探测并发上限为每个进程 32；大量后端或网络黑洞会拉长探测周期，不承诺两秒内完成全部探测。
- 每个 app 最多接受 1,024 条并发代理连接，超出后直接关闭新连接；配置交叠期间每个 app 分别计数。
- 双向拷贝支持 TCP half-close。Stop/Cleanup 取消探测、释放 listener、关闭活动连接并等待 worker 退出。

这些参数是实现常量，不扩展用户配置。没有 TCP idle timeout、连接排空、跨配置连接保留或客户端重试保证。后端进程退出、网络故障、端点更新和 Gateway 重启都可能中断连接，由客户端重连。

## 故障语义

| 情况 | TCP 行为 | HTTP/HTTPS 行为 |
| --- | --- | --- |
| Gateway 入口端口被普通进程占用 | 该 route 报监听失败，定时重试；其他 TCP route 可继续运行 | 冷启动、配置应用、恢复均不被这个错误拒绝 |
| 一个后端不可达 | 探测或拨号剔除，后续新连接选健康副本；恢复后自动加入 | 不改变 HTTP 应用状态 |
| 所有后端不可达或列表为空 | 无健康后端，新连接被关闭；listener 保留 | 不触发 HTTP reload/restart |
| 删除 route | 释放该 listener 和该 app 旧连接 | 保持原有配置生命周期 |
| 其他 app 导致整次配置加载失败 | 清理候选 TCP app 的资源，继续使用原配置 | 遵循 Caddy 原有回滚行为 |
| Gateway 蓝绿替换或 `--resume` 时 TCP bind 失败 | 候选/恢复后的对应 route 降级；不把此错误当作进程启动失败 | 原证书同步、管理端口迁移和恢复流程不变 |

`applied_generation` 表示整份配置被接受，不保证 TCP 端口已监听或后端可达。`tcp.error`、各 route 的 `error/listening` 和后端健康独立上报；不会写入 Gateway 的 HTTP/配置应用 `error`，不会为 TCP bind 错误反复重载 HTTP。

新部署包含 TCP route 时，仅该 Stack 的就绪判定还要求所有启用的 Gateway 已应用当前 generation、监听成功且每条 route 至少有一个健康后端。其他 Stack 不因这个 TCP 错误额外等待。已完成的 deployment 不会因后续 TCP 故障重新打开；运行时状态仍持续上报。

显式缩容到 0 时改为要求 listener 已绑定且后端列表为空，避免等待不应存在的健康副本；非零副本的健康要求不变。

TCP-only Service 的旧任务直接进入 Stopped，不进入现有 HTTP Draining；同一 Service 同时被 HTTP 使用时仍遵循 HTTP 的排空规则。健康检查和部署就绪不是连接排空。

## 校验与兼容

- 入口、后端端口均为 `1..=65535`。
- 后端必须是同一 Stack 的非 Job Service，且在 `expose` 或 `ports` 声明相应 TCP 容器端口。
- 同一 Stack 及不同 Stack 的 TCP listen 端口均独占；未发布的目标配置也保留端口，避免部署并发占用。
- 拒绝 `80`、`443`、`2019..=2021` 及集群配置的 HTTP listen 端口。Go app 另行保护实际 HTTP listener，防止误用可复用 socket 分摊 HTTP 流量。
- 部署需要至少一个启用的 Gateway。节点上未预见的实际 bind 冲突由每条 route 的运行时错误处理，不能依赖端口预检查消除竞争。
- 恢复快照继续接受 HTTP-only 的 v1；包含 TCP 的快照使用 v2，校验 TCP endpoint key、端口独占和路由。旧程序不支持 TCP v2，不能降级恢复包含 TCP 的集群。
- 启用 TCP 前必须升级 Controller、Node 和 Gateway 镜像。镜像须包含 `swarmlite_tcp` 和 `admin.api.swarmlite_tcp`；缺模块等结构性配置错误仍可能拒绝整次加载，不属于运行时端口故障隔离。

以上是新版本对旧配置的兼容，不代表已发布旧程序会自动忽略新字段。仓库基线 v0.1.47 的 Stack 扩展解析仍拒绝未知字段；旧 Caddy 镜像也不会忽略未注册的 app。未启用 TCP 时无需此模块；需要启用 TCP 时应先升级，不能直接把新配置推给旧版本。

共进程方案只隔离已处理的 listener、拨号和健康探测错误。panic、OOM、进程被杀或资源耗尽仍可能影响 HTTP；不承诺进程级故障隔离。Caddy 的可复用 socket 仍采用其标准行为，不能防止其他特意使用 SO_REUSEPORT 的进程加入同一端口。

## 验证

生产模块和端到端 Caddy 进程测试位于 `caddy-storage/tcp.go`、`tcp_test.go`、`tcp_fixture_test.go`。测试覆盖端口冲突、自动恢复、全部后端退出、主动健康检查、错误配置清理、half-close、真实双进程替换、HTTPS 证书加载与 `--resume`。

完整实测记录和验证边界见[验证报告](l4-port-mapping-validation.md)。`tests/gateway-l4` 保留了选择自研模块之前对固定版本 caddy-l4 的特征测试，不是生产依赖。
