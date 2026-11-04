# 原生执行身份的只读拒绝诊断

原生 `POST /api/current-execution` 把内核 socket 解析、活动 turn、worker 代次、CLI/engine PID 与启动时间、旧进程祖先、断开的 worker 和返回 schema 的拒绝统一呈现为 `403 current_execution_unverified`。宿主无法据此区分拒绝分支。

本改动保持每一个原谓词及其短路顺序。内部 `onRejection` 只记录有限原因码；它不替换证明，也不修改缺失材料。诊断来自同一次真实请求的 peer 与同步 daemon 状态，GET 不重新推断另一个调用进程。

`GET /api/sessions/:sessionId/execution-attestation` 只接受既有宿主 HMAC，并核对 session 属于当前 daemon App。该路由不进入任何公开 allowlist。记录上限为 64 个会话，超过两分钟失效，只在内存中存留。

返回 `botmux.execution-attestation-diagnostic.v1`，包括观察时间、原因码、实际 socket peer PID/start、拒绝的旧祖先（如存在）、实际 turn、worker 与证明代次、CLI/engine PID/start，以及 daemon PID/start 与 build identity。capability 只返回存在布尔值，旧进程集合只返回数量和 SHA256，不返回 capability 正文、caller、身份 token、环境变量或请求正文。

诊断用于核对守卫及实际运行版本。它不是执行许可，不能让普通诊断工作执行 BITS，也不能恢复已终态的 trigger。缺少 live guard 证据时，保持旧的 lineage 规则。

公共 native/actor 调用共享同一证明函数，PTY、tmux、engine 和其它 CLI 使用同一不变谓词。非 Linux 仍保持拒绝。测试覆盖原 native 正反门禁、每个前置拒绝、旧包装祖先、RPC engine 根、PID 回收、过期及有界缓存、鉴权、跨 App 和敏感字段排除。源码构建不认领 live launcher，也不重启 fleet。
