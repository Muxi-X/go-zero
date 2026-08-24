# go-zero (Muxi fork)

go-zero `v1.4.5` 的 fork，带一个 discov 补丁，修复 etcd 认证下 watch 周期性报 `invalid auth token` 刷屏的问题。

## 问题

etcd 用户名密码认证的 token 默认 5 分钟过期，而 clientv3 的 watch 在 token 过期后不自动刷新（etcd-io/etcd#12385），go-zero 又无限紧密重试同一个 client，导致刷屏 + CPU/磁盘耗尽。etcd 官方明确不修（workaround 是重建 client），go-zero 的修复 #5709 也未合并，因此 fork 打补丁。

## 补丁

`core/discov/internal/registry.go`（以 `[Muxi Patch]` 标记）：`watch()` 失败后 1s cooldown + 关闭旧 client、重建新 client 重新认证（对应 etcd 官方 workaround）。

## 使用

`go.mod`:

```go
require github.com/zeromicro/go-zero v1.4.5
replace github.com/zeromicro/go-zero => github.com/Muxi-X/go-zero v1.4.5-muxi
```
