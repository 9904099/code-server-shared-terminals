# 运行手册

## 构建验证

```bash
npm install --include=dev
npm test
npm run package
```

## 安装验证

1. 备份已安装的旧扩展目录和任务注册表。
2. 使用 code-server CLI 安装 VSIX，不重启服务。
3. 两个浏览器分别执行 `Developer: Reload Window`。
4. 在浏览器 A 新建 `smoke-a`，确认浏览器 B 自动出现同名右侧标签。
5. 在 A/B 新建不同任务，分别执行 `echo $PPID $$ $PWD`，确认不同任务 PID 不同、目录为 `/home/coder/aiwork`。
6. 关闭 A 的标签，确认 B 中任务继续运行。
7. 使用“结束并删除”清理 smoke 任务，确认两边标签退出且注册表删除记录。

## 只读排查

```bash
jq . /home/coder/.local/share/code-server/shared-terminals/tasks.json
tmux -L code-server-shared-tasks list-sessions
tmux -L code-server-shared-tasks list-clients
code-server --list-extensions --show-versions
```

不要输出终端正文或凭据环境变量。

## 回滚

1. 在扩展面板确认并结束不再需要的共享任务；需要保留的任务先不要 kill。
2. 卸载 `aiwork.code-server-shared-terminals` 或恢复备份扩展目录。
3. 两边执行 `Developer: Reload Window`。
4. 确认新建终端恢复为普通 Bash。
5. 只有确认没有保留任务后，才删除任务注册表并执行 `tmux -L code-server-shared-tasks kill-server`。
