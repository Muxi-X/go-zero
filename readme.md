# go-zero (Muxi fork)

go-zero `v1.4.5` 的 fork，带一个 discov 补丁，修复 etcd 认证下 watch 周期性报 `invalid auth token` 刷屏的问题。

## 问题

etcd 用户名密码认证的 token 默认 5 分钟过期，而 clientv3 的 watch 在 token 过期后不自动刷新（etcd-io/etcd#12385），go-zero 又无限紧密重试同一个 client，导致刷屏 + CPU/磁盘耗尽。etcd 官方明确不修（workaround 是重建 client），go-zero 的修复 #5709 也未合并，因此 fork 打补丁。

## 补丁

以 `[Muxi Patch]` 标记，三个文件：
- `core/syncx/resourcemanager.go`：新增 `RemoveResource`（移除并关闭单个缓存资源）
- `core/discov/internal/registry.go`：新增 `InvalidateConn`；`watch()` 失败后移除缓存 client，下次 `getClient()` 惰性创建新 client 重新认证
- `core/discov/publisher.go`：keepalive 关闭时同样失效缓存 client

思路对齐上游 PR #5709：**惰性失效 + 单飞重建**（不主动重建，避免并发/泄漏问题）。

## 使用

`go.mod`:

```go
require github.com/zeromicro/go-zero v1.4.5
replace github.com/zeromicro/go-zero => github.com/Muxi-X/go-zero v1.4.5-muxi.1
```

## 发版约定

**修改补丁时打新 tag，勿覆盖已发布 tag。** Go module 版本不可变——一旦 tag 被 proxy 缓存，内容就固定（force push 不会刷新，反而导致依赖方拉到旧内容 / checksum mismatch）。正确做法：

```bash
# 改代码 -> commit 到 muxi-patch
# 发版 -> 打新 tag（递增），不覆盖：
git tag v1.4.5-muxi.2   # 下一次
git push origin v1.4.5-muxi.2
# 项目 go.mod replace 更新到新 tag
```

历史教训：`v1.4.5-muxi` 曾因 force push 覆盖，导致 proxy 缓存初版、生产部署拉到旧补丁。详见 Muxi-X/MuXiFresh-Be-2.0 PR #53。
