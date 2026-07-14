# Code Server Shared Terminals

一个面向 Linux/code-server 的 VS Code 扩展：把服务器持久任务映射成原生终端标签，让多个浏览器窗口发现并连接同一组终端任务。

## 功能

- 资源管理器提供“共享终端任务”视图。
- 每个任务由隐藏状态栏的 tmux session 保持 PTY。
- 每个浏览器通过原生 VS Code 终端标签连接同一任务。
- 注册表变化通过文件监听和轮询同步到其他浏览器窗口。
- 关闭单个标签只会 detach；“结束并删除”经确认后终止服务端任务。
- 注册表只保存任务 ID、名称、工作目录、session 名和创建时间，不保存终端输出。

## 运行要求

- Linux x86_64
- code-server `4.127.0` / Code `1.127.0`，或兼容 VS Code API 的版本
- Node.js 22（构建）
- tmux 3.x

> 当前 `0.1.x` 版本是针对 `/home/coder` 运行布局构建的：默认工作区、注册表路径及任务启动环境均采用该布局。用于其他账号或目录前，请先调整扩展设置及 `src/task-store.ts` 中的启动环境。

## 构建

```bash
npm install --include=dev
npm test
npm run package
```

产物：`code-server-shared-terminals-0.1.2.vsix`。

## 安装

```bash
code-server --install-extension code-server-shared-terminals-0.1.2.vsix --force
```

安装后，在每个浏览器窗口执行 `Developer: Reload Window`。随后在资源管理器的“共享终端任务”视图中点击 `+` 新建任务。

## 配置

| 设置 | 默认值 | 用途 |
| --- | --- | --- |
| `sharedTerminals.autoOpen` | `true` | 自动把服务器任务映射到当前浏览器的终端列表 |
| `sharedTerminals.registryPath` | `/home/coder/.local/share/code-server/shared-terminals/tasks.json` | 共享任务注册表 |
| `sharedTerminals.defaultCwd` | `/home/coder/aiwork` | 新任务默认工作目录 |

## 使用边界

- 两个浏览器连接同一个任务时，会看到并操作同一 PTY；不要同时向同一个交互式任务输入。
- 两边需要独立工作时，应创建两个不同任务。
- 目前仅支持 Linux/code-server。

## 架构与运维

- [架构说明](docs/architecture.md)
- [运行手册](docs/runbook.md)

## License

[MIT](LICENSE)
