# Internal task results and conversation replies

An authenticated trigger can choose `suppressFinalOutput: true` for its internal
result. This affects the automatic final reply for that execution. It does not
choose which conversation messages are tasks, advance project dependencies, or
deliver a result to another worker.

When the worker confirms that an executing turn adopted another input, the new
turn inherits the existing final-output policy. Pending reservations, queued
independent requests, and later conversations keep their own output policy.
Project membership and whether a message came from another bot do not make an
independent conversation reply silent.

For asynchronous UI tasks that need public progress or a handoff, set
`allowChatMessages: true` together with `asyncReturnSessionId: true`. The task can
then use `botmux send` in its bound conversation while its internal final result
remains isolated. This option does not grant access to a different conversation
or enable sending from a headless task.

The caller's task runtime should persist structured results, arrange subsequent
work, and decide which progress to publish. A natural-language final answer is
not a dependency transition or evidence that a task completed. Updating the
runtime does not automatically replay results from historical executions.
