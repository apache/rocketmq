# POP KV Group Commit

POP KV 的 ACK、CHANGE、POP CK、缓存转储和 revive 写入共用一个 `PopKVGroupCommit` 线程。请求线程只序列化、入队和注册 Future 回调，不执行 RocksDB 同步写。持久化仍使用 `sync=true`、`disableWAL=false`。

## 提交与响应

1. 提交时将记录编码为不可变的 key/value 快照，避免排队期间原始记录被修改。
2. 写线程取到第一个请求后立即收集当前队列中的请求，不主动等待凑批。
3. 按入队顺序将 put/delete 加入一个 WriteBatch。一个请求的原子更新不会拆到多个批次。
4. `db.write` 成功返回后，由 `PopKVCompletion_` 线程池逐请求完成异步 Future；整批写失败则整批异常完成。响应或后续业务回调不会占用写线程。
5. ACK 等待自身写入；Batch ACK 等待所有参与记录的 Future。CHANGE 即使关闭旧的 `appendCkAsync` / `appendAckAsync` 开关也使用异步响应。POP CK 入库失败会回复 `SYSTEM_ERROR` 并释放消息缓冲区及消费锁。

选择独立写线程，是为了避免同步 WAL 写或 RocksDB write stall 占住 ACK 请求线程。完成回调使用独立线程池，`popConsumerKVCompletionThreadPoolNums` 默认 4，必须为正数。每个请求单独调度完成任务，使同一 WriteBatch 中的回调也能并行执行，避免 POP 后续处理阻塞其他请求的回调。写入顺序保持不变，不保证独立请求的回调执行顺序；业务依赖通过 Future 链表达。线程池随 POP KV 存储排空并关闭，避免依赖 Broker 关闭时先行停止的发送回调池。

## 上限与生命周期

- 最多 4096 个等待写入或等待开始完成通知的请求，包含已落盘但排队等待回调线程的请求。开始完成通知时释放预算，允许回调再次提交写入；此外至多有线程池大小个正在执行回调的任务。回调积压仍受此上限约束。
- 排队及在途请求的编码 key/value 字节预算为 64 MiB；不包括 Java 对象、序列化临时对象和网络缓冲区。
- 每批最多 128 个请求，通常不超过 1 MiB 编码数据。单个超过 1 MiB 的原子请求独占一批，仍受总字节预算限制。
- 预算耗尽或关闭后立即返回异常 Future，不等待队列空间，也不回退到请求线程写盘。
- 关闭时先停止接收请求，再等待写线程和完成回调排空，最后关闭 RocksDB 的 native handles。不会中断正在执行的 native write。

`PopConsumerKVStore.writeRecords/deleteRecords` 保留同步语义，供缓存维护和迁移等后台流程使用；它们也向同一队列提交，通过内部 Future 等待持久化，不直接写 RocksDB。内部 Future 在写线程完成，避免异步回调调用同步存储 API 时相互等待。异步 API 的后续写入应使用 `thenCompose` 串联，不应在完成池中阻塞等待另一个公开异步 Future，否则所有完成线程均被占用时仍会互相等待。

## CK 一致性

- 未启用缓存：CHANGE 的新 CK put 和旧 CK delete 合为一个原子请求。新旧 key 相同只做 put，防止删掉新记录。
- 启用缓存：保留先持久化新 CK、再删除旧缓存 CK 的顺序；如果旧 CK 不在缓存，再异步删除其存储记录。第一阶段失败不会移除旧缓存 CK。
- revive：失败重试 CK 的写入和本轮旧 CK 删除合为一个原子请求，成功后才能推进扫描游标。扫描线程可以等待自己的提交完成，ACK/CHANGE 请求线程不会跟着等待。
- `changeInvisibilityDuration` 现在返回 `CompletableFuture<Void>`，调用方必须链式处理完成/异常，不能再忽略结果。

## 验证

```shell
# 使用 JDK 21
git submodule update --init rocketmq-apis
mvn -B -pl broker -am test \
  -Dtest='PopConsumer*Test,PopKVAsyncProcessorTest,AckMessageProcessorTest,ChangeInvisibleTimeProcessorTest,PopMessageProcessorTest' \
  -Dsurefire.failIfNoSpecifiedTests=false -DfailIfNoTests=false -Djacoco.skip=true
```

本次执行结果：86 项测试，0 失败，0 错误，1 项原有跳过；Checkstyle 和 SpotBugs 均通过。

检查包含 Checkstyle、SpotBugs、原有 POP 回归、合批顺序和批大小限制、写失败传播、队列满拒绝、关闭排空、回调隔离、同批回调并行、回调再次提交同步/异步写入、完成池繁忙时限流和关闭排空、真实 RocksDB 重开校验、同 key 更新、缓存 CK 保护、扫描游标保护，以及处理器成功/失败响应时序。跳过 JaCoCo 是因为仓库的 0.8.5 agent 不支持 JDK 21；没有跳过上述静态检查或测试。

这些验证不等同于完整 Broker/Proxy 压测、进程崩溃注入或长时间 compaction 验收；尚未给出本补丁的吞吐量提升结论。
