# go-zero (Muxi fork)

go-zero `v1.4.5` 的 fork，带一个 discov 补丁，修复 etcd 认证下 watch 周期性报 `invalid auth token` 刷屏的问题。

## 问题

etcd 用户名密码认证的 token 默认 5 分钟过期，而 clientv3 的 watch 在 token 过期后不自动刷新（etcd-io/etcd#12385），go-zero 又无限紧密重试同一个 client，导致刷屏 + CPU/磁盘耗尽。etcd 官方明确不修（workaround 是重建 client），go-zero 的修复 #5709 也未合并，因此 fork 打补丁。

## 补丁

涉及四个源文件（其中 `registry.go`、`publisher.go` 的改动带 `[Muxi Patch]` 标记，`resourcemanager.go`、`statewatcher.go` 无标记）：
- `core/syncx/resourcemanager.go`：新增 `RemoveResourceIf`（仅当缓存仍是该 client 时才移除并关闭，避免误关并发重建出的新 client）
- `core/discov/internal/registry.go`：新增 `InvalidateConn`；`watch()` 失败后仅在 client 已关闭或认证失效（`invalid auth token` / auth revision 过期 / user 为空）时按 id 失效缓存 client，下次 `getClient()` 惰性创建新 client 重新认证；compaction 等 watch 自身的失败改为重新 load 后重试，不打断共享同一 client 的其他 watcher/publisher
- `core/discov/internal/statewatcher.go`：watch 改用可取消 ctx（`cli.Ctx()`），client 关闭时退出，避免 goroutine 泄漏
- `core/discov/publisher.go`：keepalive 关闭时按 id 失效缓存 client（resume 时不失效：连接恢复后的 unary 调用会自动刷新 token，关掉共享 client 会打断其他 watcher）
- `core/discov/internal/registry.go`（`load()`）：client 的 Ctx 已取消（已被并发失效源关闭）时立即返回，避免对死 client 无限 Get 导致 watcher 永久卡死（否则 `load()` 唯一出口只在连接状态变化时关闭，token 失效不算，故不自愈）；cooldown 可被代次关闭打断；权限/认证等不可恢复的 Get 错误返回给 `monitor()`/`NewSubscriber`，其余错误继续重试
- `core/discov/internal/registry.go`（`reload()`/`monitor()`）：watcher 按代次传入 `done`，`reload()` 不再持锁 `Wait`（修复与 `handleChanges()` 的互等死锁），并用 `started` 集合保证每个 key 每代只有一个 watcher（修复 `monitor()` 与 `reload()` 交错时的重复/丢失 watcher）

思路对齐上游 PR #5709：**惰性失效 + 单飞重建**（不主动重建，避免并发/泄漏问题）。

## 使用

`go.mod`:

```go
require github.com/zeromicro/go-zero v1.4.5
replace github.com/zeromicro/go-zero => github.com/Muxi-X/go-zero v1.4.5-muxi.7
```

## 版本历史

- `v1.4.5-muxi`：初版（主动重建，弃用）
- `v1.4.5-muxi.1`：惰性失效
- `v1.4.5-muxi.2`：修复重建时 client 被并发 Close 的竞态
- `v1.4.5-muxi.3`：修复 `load()` 对已关闭 client 无限重试导致 watcher 永久卡死（生产实测：etcd 认证 token 过期触发失效风暴后不自愈）
- `v1.4.5-muxi.4`：失效缓存 client 时按 id 判断，仅在仍是失败的那个 client 时才移除，避免误关并发重建出的新 client 而打断其 watcher
- `v1.4.5-muxi.5`：watch 失败仅在 client 已关闭或 auth token 过期时才失效缓存 client（compaction 等 watch 自身失败改为重新 load 重试，不再打断共享 client 的其他 watcher/publisher）；publisher resume 不再失效共享 client
- `v1.4.5-muxi.6`：重建出的 client 若已被其他失效源关闭，切换过去并在下一轮按身份清掉它（原兜底逻辑实际无法恢复）；auth 失效判定补充 `ErrAuthOldRevision` / `ErrUserEmpty`
- `v1.4.5-muxi.7`：watcher 改为按代次传 `done`；`reload()` 锁外 `Wait`，修复与 `handleChanges()` 的互等死锁；`monitor()`/`reload()` 用 `started` 集合保证每个 key 每代只有一个 watcher；`load()` 的 cooldown 可被打断，权限/认证类永久错误返回给 `NewSubscriber` 而不是无限重试

## 发版约定

**修改补丁时打新 tag，勿覆盖已发布 tag。** Go module 版本不可变——一旦 tag 被 proxy 缓存，内容就固定（force push 不会刷新，反而导致依赖方拉到旧内容 / checksum mismatch）。正确做法：

```bash
# 改代码 -> commit 到 muxi-patch
# 发版 -> 打新 tag（递增），不覆盖：
git tag v1.4.5-muxi.7   # 下一次
git push origin v1.4.5-muxi.7
# 项目 go.mod replace 更新到新 tag
```

历史教训：`v1.4.5-muxi` 曾因 force push 覆盖，导致 proxy 缓存初版、生产部署拉到旧补丁。详见 Muxi-X/MuXiFresh-Be-2.0 PR #53。
