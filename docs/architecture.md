# 架构

## 目标

在不修改 code-server 源码的前提下，让多个浏览器窗口看到同一组终端任务，并在右侧原生终端列表中打开这些任务。

## 组件

```text
浏览器 A extension host ─┐
                         ├─ tasks.json ─ TaskStore ─ tmux socket
浏览器 B extension host ─┘                         ├─ task A PTY
                                                   └─ task B PTY
```

- `TaskStore` 对注册表使用目录锁和原子替换，避免两个 extension host 并发写坏 JSON。
- 每个任务对应一个独立 tmux session；session 状态栏关闭。
- 每个浏览器使用原生 `createTerminal` 以 `tmux attach-session` 连接任务，因此右侧显示正常终端标签。
- 文件监听器检测注册表变化；`autoOpen=true` 时在当前工作台补齐缺失标签。

## 一致性语义

- 任务清单是服务器共享事实。
- 标签 UI 是各浏览器本地对象，由插件根据共享事实重建。
- 关闭标签只 detach；删除任务会 kill 服务端 session，并让其他客户端连接退出。
- 同一任务支持多个 attach，但多客户端同时输入由操作者协调。

## 安全边界

- 注册表权限 `0600`。
- 仅允许绝对工作目录，并在 UI 创建前验证目录存在。
- 删除任务需要模态二次确认。
- 不开放新端口，不引入独立 WebSocket 服务，不复制 shell 输出到注册表。
