# HashedWheelTimer 任务隔离模式（Task Isolation Mode）交付说明

## 1. 开关与用法

- 新增一个构造器（其余构造器全部保留，签名与行为不变）：

  ```java
  public HashedWheelTimer(
          ThreadFactory threadFactory,
          long tickDuration, TimeUnit unit, int ticksPerWheel,
          boolean leakDetection, long maxPendingTimeouts,
          Executor taskExecutor, boolean taskIsolationEnabled)
  ```

- `taskIsolationEnabled` 默认按 `false` 处理：不传该参数时，所有现有构造器
  原样委托，分配、同步、执行路径与改动前完全一致（到期任务仍在 worker 线程上
  内联执行）。
- 传 `true` 时：`taskExecutor` 参数被忽略，定时器内部自建一个有界线程池执行
  到期任务；此时不要再传自定义 executor。

## 2. 隔离后的执行模型

- worker 线程只负责推进时间轮：到 tick → 取本 tick 到期的任务 → 逐个派发到
  内部线程池 → 立即进入下一个 tick。任务体（`TimerTask.run`）永不运行在
  worker 线程上。
- 内部线程池：`ThreadPoolExecutor`，核心/最大线程数都固定为
  `MAX_ISOLATION_THREADS = 8`，队列为无界 `LinkedBlockingQueue`，
  `allowCoreThreadTimeOut(true)`、空闲 60s 回收，线程通过用户提供的
  `ThreadFactory` 创建（线程名追加 `-isolatedTimeout` 后缀）。线程数按任务
  排队压力增长、封顶 8，不会按任务数起线程；不引入任何新依赖，编译目标仍为
  Java 8。
- 派发顺序 = 提交顺序：同一 tick 到期的任务按其在桶链表中的顺序（即被
  `newTimeout` 提交的顺序）提交给线程池，提交动作之间存在 happens-before
  关系，池内任务可见的提交次序确定。

## 3. 验收口径（验收按本节卡）

以下数字针对常见 CI 负载机器；测试中 `tickDuration = 10ms`。

1. **慢任务不拖后续批次**：一个任务 `sleep(500ms)`，与它**不同到期时刻**
   （后一个 tick 或更晚）到期的任务，其实际开始时刻相对各自到期时刻的偏差
   **≤ 200ms**（含一个 tick 的取整误差 10ms、派发与线程唤醒开销）。
2. **线程封顶**：任意负载下定时器内部新增线程数 ≤ 8。
3. **取消保证**：任务在真正开始执行（`TimerTask.run` 的第一行）之前调用
   `cancel()` 一定成功且任务体绝不执行——即使已经被派发到线程池甚至已经
   在线程池队列里。任务开始执行后 `cancel()` 返回 `false`，运行中的任务不会
   被强行终止（语义与开关关闭时一致）。
4. **停止上界**：`stop()` 在
   **join worker（≤ 一个 tickDuration，默认构造器下 ≤ 100ms）+ 至多 30ms**
   之内返回；有任务卡住不返回时也不陪着卡。
5. **停止返回集合**：只包含“尚未开始执行”的任务（时间轮里没到期的 +
   已派发但尚未开始跑的，包括线程池队列里的）；已经开始执行的任务不在集合中。
   集合中的每个 `Timeout` 都处于 cancelled 状态。
6. **计数与上限**：`pendingTimeouts()` 口径不变——任务在到期派发瞬间
   （执行前）减 1；`maxPendingTimeouts` 超限拒绝的行为不变。

> 饱和例外：如果同时有 ≥ 8 个任务各自长时间占用线程，第 9 个及之后的任务
> 必须在线程池队列里等待。这是“线程数封顶”的必然结果，不属于验收口径 1 的
> 覆盖范围（口径 1 的慢任务场景只有 1 个慢任务，其余到期任务都有空闲线程）。

## 4. 关键取舍：派发原子性 vs. 同刻顺序，选了隔离

任务交给别的线程后，“它到底算跑了还是没跑”不再有一个天然的原子时刻，而
`stop()` 的返回集合和 `pendingTimeouts()` 都依赖这个时刻。要想在这种情况下
仍严格保证“同一到期时刻提交的任务，跨线程也按提交顺序开始执行”，只能让
同一批（同一 tick）任务串行派发/串行开始（例如整批包成一个 `Runnable`，或
用 start-latch 串成链）。那样的话：

- 慢任务仍会挡住**同一批里排在它后面**的任务（只是挡不住下一批）；
- 在封顶线程池 + latch 链方案下，慢任务还可能长期占着链上的一个线程槽位，
  链深超过线程数时连“下一批不被挡”也会退化成饥饿。

因此本实现选择偏向**隔离**：同批任务**并行**派发到有界线程池，任何一个任务
睡 500ms 都不会拖住其他到期任务（口径 1）。代价是：

- **同一到期时刻的任务只保证派发顺序与提交顺序一致，不保证跨线程的开始
  （start）顺序**。线程调度可能让后派发的任务先开始。这是本开关刻意放弃的
  属性，需要严格 FIFO 开始顺序的调用方应保持开关关闭（内联执行天然保序）。

“是否已开始执行”这个原子时刻由 `HashedWheelTimeout` 的状态 CAS 显式构造：
`ST_EXPIRED`（已派发未开始）→ `ST_RUNNING`（任务线程 CAS 成功的一刻即
“开始执行”）。`cancel()` 与 `stop()` 与该 CAS 竞争，从而得到确定的判定，
而不是依赖线程调度时机：

- `cancel()`：`ST_EXPIRED → ST_CANCELLED` 成功即取消，任务线程随后的
  `ST_EXPIRED → ST_RUNNING` CAS 失败，任务体跳过；若任务线程先抢到
  `ST_RUNNING`，`cancel()` 返回 `false`。
- `stop()`：worker 退出后收集时间轮上未到期任务，再 `shutdownNow()` 取回
  线程池队列中的任务，并扫描已派发集合；凡能 CAS 成 `ST_CANCELLED` 的都算
  “未开始”，进返回集合；CAS 失败说明已 `ST_RUNNING`，不进集合。
  `shutdownNow()` 会 best-effort 中断正在运行的任务（与开关关闭时 `stop()`
  中断 worker 线程的既有行为一致），但不等它们结束。

## 5. 其它语义

- 已开始执行的任务抛异常仍只记录 warn 日志，不影响其它任务（异常发生在
  独立线程上，更不会影响 worker 推进）。
- `isExpired()` 在任务已开始/已派发后返回 true（`ST_EXPIRED` 与
  `ST_RUNNING` 都视为 expired）。
- 取消成功仍会回调 `TimerTask.cancelled(Timeout)`：开关关闭时由 worker 在下
  一个 tick 回调；隔离模式下“已派发未开始”的取消在调用线程直接回调，
  `stop()` 产生的取消也会回调。
- 隔离线程池随 `stop()` 关闭（`shutdownNow`），定时器停止后不可再启动，
  与既有行为一致。
