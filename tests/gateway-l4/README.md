# Gateway L4 故障行为验证

这是独立 Go module，不改变生产 `caddy-storage/go.mod`。测试将 Caddy `v2.11.4`、caddy-l4 `v0.1.2` 和本仓库现有 Caddy 模块编入测试程序，每个 Caddy 实例都运行在真实子进程中，使用独立配置目录和 Admin 地址。

测试覆盖端口占用、配置拒绝后残留监听、两个后端全部退出、零后端、恢复后端、冷启动、`--resume`，以及 HTTP/L4 分进程后的隔离和失败恢复。

**这是历史对照实验，不是当前原生模块的验收。** 日志中的 `LIMITATION` 是固定版本 caddy-l4 的已复现问题；测试整体 PASS 表示成功复现并检查了这些行为，不表示问题已经修复。当前生产实现使用本仓库自研的 `swarmlite_tcp`，不依赖 caddy-l4；其测试位于 `caddy-storage/tcp_test.go`。

## 运行

在当前主机执行：

```sh
cd tests/gateway-l4
go test -v -count=20 -timeout=120s ./...
```

在隔离的 Linux/arm64 容器中执行（本次验证使用此路径）：

```sh
cd tests/gateway-l4
probe_dir="$(mktemp -d /tmp/swarmlite-gateway-l4.XXXXXX)"
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go test -c -o "$probe_dir/gateway-l4.test"
docker run --rm \
  --mount "type=bind,src=$probe_dir,dst=/probe,readonly" \
  --entrypoint /probe/gateway-l4.test \
  busybox:1.36 -test.v -test.count=20 -test.timeout=120s
```

Linux/amd64 Docker 主机将 `GOARCH` 改为 `amd64`。容器不映射宿主机端口、不使用 host network、不访问现有 Gateway 或业务容器；所有流量均在测试容器内完成。测试退出时子进程被停止，容器通过 `--rm` 自动移除。临时目录只保留生成的测试二进制，不写入仓库。

测试使用环回地址和临时高端口，因此验证的是 Caddy 的实际进程、配置加载和转发行为，不覆盖 Docker host network、Swarmlite Agent 调度、跨节点网络或 HTTP 蓝绿升级。

成功配置加载后的检查等待服务就绪，并记录过渡期请求错误；这不是 HTTP 零中断重载测试。旧配置在失败加载后的可用性检查，以及独立 L4 故障后 HTTP 的可用性检查不使用此类重试。

结果和方案调整见 [验证报告](../../docs/design/l4-port-mapping-validation.md)。
