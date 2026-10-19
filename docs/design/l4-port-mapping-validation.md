# Gateway TCP 端口映射：故障验证记录

日期：2026-10-19。当前实现选择本仓库的原生 `swarmlite_tcp` app，在现有 Caddy 进程内按 route 隔离可预期的 TCP 运行时错误，保留健康检查。验证期间未发布版本或改动线上 Gateway。

## 当前原生实现

环境为 Caddy `v2.11.4`、本仓库 `caddy-storage` 模块。真实 Caddy 子进程使用隔离的 admin 地址、XDG 目录和 autosave，测试实际调用 `run --config`、`/load`、`run --resume`；不是仅检查 JSON 或 mock listener。

测试由 `caddy-storage/tcp_test.go` 和 `tcp_fixture_test.go` 提供。Linux 测试由 macOS/arm64 交叉编译静态二进制，在 Docker Linux/arm64 的 `busybox:1.36` 容器内执行；容器不映射宿主端口、不使用 host network、不连接生产 Gateway。

| 场景 | 已验证行为 |
| --- | --- |
| TCP 端口实际被另一 socket 占用 | 共享进程冷启动、HTTP 两条转发、其他 TCP route 和 `--resume` 正常；错误仅出现在该 route；释放占用后自动恢复监听 |
| 热更新增加正常路由和冲突路由 | `/load` 成功，HTTP 新配置生效，正常 TCP 可用，冲突路由降级；删除路由后端口释放 |
| 两副本全部退出与恢复 | 不发送客户端流量也会由主动探测剔除；只选健康副本；全部不可用时新连接关闭；端点原地恢复后自动重新加入 |
| 零后端、端点替换、恢复 | 空 upstream 配置可加载并恢复；替换地址后新连接到新后端；HTTP 正常 |
| 试图与实际 HTTP 监听端口重叠 | TCP route 单独拒绝，HTTP 保留；不会复用 HTTP socket 抢流量 |
| 其他 app 启动失败 | 连续十次拒绝候选配置后旧 HTTP/TCP 保留；候选端口释放，TCP 状态回到旧 app，无隐藏 listener |
| half-close 和有长连接时停止 | 双向响应可完整传输；退出不会被活动连接无限阻塞 |
| 真实新旧 Gateway 进程交叠替换 | HTTP/TCP listener 可复用；候选额外 TCP 冲突不妨碍 HTTP、旧进程退出、管理端口迁移或新进程 `--resume` |
| HTTPS 与 TCP 冲突同配置 | 加载已有 PEM 证书的 HTTPS 转发、停止和 `--resume` 成功，即便 TCP listener 被占用且后端已退出 |

端口占用由实际监听 socket 制造，后端退出由关闭后端监听制造；健康恢复在同一地址重新监听。HTTP 在 TCP 故障发生后直接采样断言。HTTP 自身成功重载/启动后允许等待就绪并记录重试，不据此宣称 HTTP 重载零中断。

验证命令：

```bash
cd caddy-storage
go test -race ./... -timeout=120s
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go test -c -o /tmp/native-tcp.test
# 将测试二进制只读挂载到隔离的 Linux 容器中：
# /probe/native-tcp.test -test.run=^TestTCP -test.count=3 -test.timeout=120s
```

Go 竞态检查通过；上述九个 TCP 场景在 Linux 容器中各重复三次通过。Rust workspace 的测试与严格 Clippy、UI 测试/类型检查/构建另行验证解析、端口分配、端点过滤、恢复快照、TCP-only 退役、按 Stack 就绪判定及独立状态展示。

## 配置兼容性回归

- 旧 Stack 不含 `x-swarmlite` 或 `tcp_routes` 均可解析；`tcp_routes: []`、YAML 空值和 `null` 均等价于未启用。
- 缺省/空 TCP 扩展不出现在序列化结果中，不生成 Caddy TCP app；已有 HTTP 路由的 Caddy JSON 和 v1 恢复快照与缺省配置完全一致。
- `x-swarmlite`、TCP route/backend 的未知字段在 YAML 和 JSON 中忽略，不写回配置。已知字段类型错误、缺失必填字段、非法端口仍拒绝。
- 旧 Gateway report 未含 TCP 字段时可读，重新序列化保持原有 wire shape；未来 report 扩展字段仍可忽略。

这是本地源码和回归测试证据，未连接或修改线上实例；不能改变已发布旧二进制的严格解析行为。启用 TCP 仍需升级至含模块的 Gateway 镜像。

## 整集群 E2E

2026-10-19 补充 `tests/cluster-e2e`，在 OrbStack Linux/arm64 中启动三个独立 Docker-in-Docker 节点，每个节点都有自己的 dockerd、容器清单、数据目录和网络命名空间。Controller 节点和第二个节点各运行一个真实受管 Gateway；业务容器只能调度到第三个节点。Gateway 使用各自节点的 host network，真实执行 Docker 端口发布、配置下发、心跳、健康检查和蓝绿替换，不是共享宿主 Docker socket 的模拟节点。不映射外部宿主端口，不操作原有容器。

升级基线为提交 `d9610aa4c13861a104a323dde665ba0f678c9b05`（v0.1.47）的干净源码及旧 Gateway 模块，新版本为当前工作树；二者分别构建 Linux 静态可执行文件。CLI 版本号均为尚未变更的 0.1.47，实际通过不同二进制和镜像 ID 验证替换，不能仅凭版本字符串判定升级。共用预构建静态 UI 资源，未测试 UI 版本升级。Caddy 为 v2.11.4，节点内 Docker 为 29.9.1。

修复后用全新集群连续验收两轮，均为 **15/15 通过**。两轮分别采集 508 / 504 次有后端响应内容校验的 HTTP 请求，共 1,012 次；失败分别为 1 / 0 次。唯一失败发生在首轮显式 `docker restart` Gateway 容器的窗口，恢复后正常。TCP 端口占用、后端失败、配置发布和蓝绿镜像替换阶段未观察到 HTTP 采样失败。这是采样结果，不是零中断证明。

| 整集群场景 | 结果 |
| --- | --- |
| 旧 HTTP-only 集群，先升级 Worker/Node、最后 Controller，再升级 Gateway 镜像 | HTTP 业务容器 ID 保留；无 TCP 配置时不生成 TCP app；HTTP 转发正常 |
| 两个 Gateway → 第三个节点的两副本 | 实际动态发布端口与 Controller/Gateway 端点一致；两个不同响应证明轮询；未知扩展字段忽略 |
| TCP 仍可连接但应用健康检查失败 | Controller 剔除该端点并自动替换副本；其他转发保持正常 |
| 仅一个 Gateway 的宿主 TCP 端口被占用 | 仅对应 TCP route 失败、所属 Stack 等待；HTTP 配置 ACK 正常，新 HTTP Stack 可部署，其他 TCP route 可用 |
| 有端口冲突时重启受管 Gateway | `--resume` 成功恢复 HTTP 和正常 TCP route；释放占用后无需重新部署即可恢复 |
| 暂停 Worker Agent，再停止全部 TCP 业务副本 | Caddy 主动探测剔除所有后端，新连接失败；HTTP 和另一个 Stack 的 TCP 正常；恢复 Agent 后自动替换任务、更新动态端口并恢复 |
| 0 副本 → 2 副本 | 0 副本时 listener 保留、空后端关闭新连接，部署可完成；扩容后恢复 |
| Controller 重启、TCP-only Service 重启 | 已运行 Gateway 转发保留，DB 配置恢复，Service 重启完成且不等待 HTTP drain |
| TCP 已启用且新增 route 仍存在 bind 冲突时替换 Gateway 镜像 | 实际候选启动、晋升、旧容器移除完成；HTTP/正常 TCP 可用；释放占用后新 route 恢复 |
| 删除 TCP Stacks | TCP app 移除，两个 Gateway 的四个入口端口都能被新 host-network listener 绑定，无残留；HTTP 保留 |

E2E 发现并修复一个问题：显式缩容到 0 后，部署就绪原先仍要求健康后端，无法达到 Healthy。现在只有明确 `replicas: 0` 时接受“监听成功且后端为空”；非零副本继续要求健康后端，端口冲突、旧后端未移除或未 ACK 的配置仍不算就绪。先补充 Rust 回归并复现失败，再修复、通过 Controller 106 项测试、workspace 测试及严格 Clippy，并重编 Linux 二进制完成上述整集群验收。

复现入口见 `tests/cluster-e2e/README.md`。脚本保留逐阶段状态快照、最终报告、Controller DB、Node/Gateway 日志，在成功或异常时清理本次创建的三个节点、匿名卷和唯一网络；不执行全局 prune。测试尚未发布版本或修改线上集群。

## 早期 caddy-l4 对照实验

保留 `tests/gateway-l4` 作为独立特征测试：Caddy `v2.11.4` + caddy-l4 `v0.1.2`，七个场景在 Linux 容器中各重复二十次。这些结论只针对固定版本；生产未引入 caddy-l4。

| 场景 | 对照实验结果 |
| --- | --- |
| 共享 HTTP/L4 进程，冷启动或 `--resume` 时 TCP 端口冲突 | 整个 Caddy 非零退出，HTTP 不可用 |
| 共享配置 `/load`，一个 listener 成功、后续 bind 失败 | 返回 400，旧配置/autosave 保留，但候选第一个 listener 未释放且仍转发，之后成功加载再移除也未清理 |
| 所有 TCP 后端退出 | 受影响连接失败，其他 HTTP/TCP 未观察到影响 |
| 空 upstream 的 proxy handler | `/load` 拒绝；必须改为 close handler |
| HTTP 与 L4 独立进程 | L4 启动/恢复失败不影响 HTTP；单独重启 L4 可清理残留 listener |

固定版本中，caddy-l4 的 app 启动在部分 bind 后失败，没有完整释放该次创建的 listener；Caddy 不会对启动失败的 app 调用正常 Stop。证据位于独立测试及固定版本源码：[caddy-l4 app](https://github.com/mholt/caddy-l4/blob/v0.1.2/layer4/app.go)、[Caddy 配置运行流程](https://github.com/caddyserver/caddy/blob/v2.11.4/caddy.go)。

最初据此考虑独立 L4 容器；最终选择自研最小 app，让可预期的 bind/后端错误变为运行时状态，并实现候选 app 的 Cleanup。原生模块的回滚测试重复验证资源清理，不能用旧 caddy-l4 的结果推断原生 app 的行为。

## 验证边界

- 独立进程测试和整集群 Docker 蓝绿替换均已覆盖；未证明所有既有 TCP 连接在配置更新、重启或替换时都不中断，客户端仍需重连。
- HTTPS 验证使用已有测试证书，未连接 ACME；不证明证书签发/续期或证书 KV 交接的全流程。
- 已运行三个逻辑 Linux 节点的真实部署、跨节点网络和 host network 端口争用；三者仍共用同一 VM/kernel，不能替代物理跨主机网络和真实防火墙测试。connect/dial 超时已实现，但尚无静默丢弃 SYN 的网络黑洞实测结论。
- 没有做持续压测、资源耗尽、OOM/panic、长时间运行或所有连接零中断证明。共进程仍有共同故障域；TCP 重载/退出允许断连接。
- 原有需要 Linux+Docker 的部分 Rust 集成测试在普通 workspace 测试中保持 ignored，未将其计为已通过的场景。

本地整集群验收已完成。上线时需使用包含新模块的 Gateway 镜像、升级 Controller/Node，并在实际网络中小范围验证防火墙及跨主机可达性后再启用 TCP。
