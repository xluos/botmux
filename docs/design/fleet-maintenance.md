# 受控整组维护合同

维护协议只接受既有 trusted-host HMAC。宿主安装合同须预置私有、固定的 operation grant，完整绑定 task、work、execution、actor、App、session、scope、trigger、原 Ask、审批和审查摘要。请求不能自行提供运行权限；构建源码不会安装 grant。

`/api/fleet-maintenance/:action` 提供 prepare、read、abort，以及有界 driver 的 run、resume、reconcile、reconcile-drain、finish-release 协议。当前宿主适配器明确拒绝执行：旧 supervisor 没有跨重启持久 drain bootstrap。必须先取得真实旧进程已加载的保护证明，不能先无保护重启来安装保护。

持久状态由固定 operation journal 和 active 指针组成，写入先 fsync 再 rename，再 fsync 父目录；双份不一致、缺一份、残留临时文件或锁都拒绝准入。锁不按 TTL 抢占。旧 operation 的终态保留，不能复用其 ID。

新宿主的 root turn admission 与 mutation 在等待前和重新取得 gate 后读取持久 fence。已获准的同一 turn 可以自然完成。这里的 fence 不能代表旧版本已经具备保护，也不能代替全入口入站库存与队列的真实 drain 确认。

一次 driver 固定完整 fleet、daemon generation、配置、launcher、dist、解释器、user-systemd unit 与 invocation/PID/start/cgroup。每个 source step 在实际派发前落盘唯一 intent；超时只记 unknown，不 kill、不重派。unknown 只允许原 driver 对同 operation/step 做一次实际回执对账；对账成功后可以继续尚未签发的 step。

drain 同样先写 pending intent。中途 abort 先记 aborting，尚未结束的 drain 不允许提前 release；不明 drain 需由原 operation 的真实状态回读核销。部分 release 保持持久 fence，逐 App 恢复尚未确认的 release，全部确认后才写终态。激活、回退、再启用的每一步都重新核对自然 terminal、来源、配置和真实进程/worker generation。

当前版本交付协议核心、拒绝执行的宿主入口及隔离组件测试。真实 user-systemd one-shot driver、旧版本 bootstrap、全入口库存确认、原生安全宿主切换与回退均未完成；这些缺项保持安装拒绝，不把模拟回执算实际维护或业务验收。
