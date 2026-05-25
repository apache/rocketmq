# RocketMQ Flaky Test 检测方案

## 背景与目标

RocketMQ 主干 CI 经常出现间歇性测试失败，导致开发者对红色构建产生信任疲劳，真正的回归问题容易被掩盖。本方案通过大规模重复执行统计失败率，对不稳定方法（≥1%）标记 `@Ignore` 恢复 CI 可靠性，同时保留数据为后续修复提供优先级依据。

## 方法论来源

- **Google** — [Flaky Tests at Google and How We Mitigate Them](https://testing.googleblog.com/2016/05/flaky-tests-at-google-and-how-we.html)（2016）：提出 "deflake"（重复执行 N 次统计失败率）和 "quarantine"（将不稳定测试从主线 CI 隔离）。内部数据：约 1.5% 的测试存在 flakiness，16% 曾经 flaky 过。
- **Meta** — [Predictive Test Selection](https://engineering.fb.com/2018/11/21/developer-tools/predictive-test-selection/)（2018）：通过 aggressive retry 区分 flaky 失败与真实回归。
- **Spotify** — [Test Flakiness Methods](https://engineering.atspotify.com/2019/11/test-flakiness-methods-for-identifying-and-dealing-with-flaky-tests/)（2019）：重复执行 + 隔离 + 追踪的三阶段治理框架。

## 核心思路：三层漏斗

采用"粗筛 → 精筛 → 定位"逐步缩小范围，避免在全量方法级别浪费算力：

```
第一层：模块级（16 模块 × 100 次）→ 筛出有失败的模块
第二层：类级（仅不稳定模块中的测试类 × 100 次）→ 筛出有失败的类
第三层：方法级（仅不稳定类中的测试方法 × 100 次）→ 精确定位每个方法的失败率
```

每层执行完后分析 Surefire XML 报告，输出不稳定列表作为下一层的输入。标记后重新全量执行验证，如仍有新 flaky 出现则循环标记 + 验证，直到零失败。

## 执行架构

- **控制节点（本地）**：编排任务分发、结果收集、数据分析
- **工作节点（10 台 ECS，16C 64G）**：每台最多 4 个 Docker 容器并行执行测试，互不干扰

## 执行流程

```
1. 构建  → Docker 内 JDK 8 编译 RocketMQ，打包为测试镜像
2. 分发  → 内网中转分发镜像到所有工作节点
3. 派发  → 生成任务列表，均匀拆分到各节点，启动 worker
4. 收集  → 轮询等待完成，回收 Surefire XML 报告
5. 分析  → 解析 XML，统计失败次数和失败率
6. 标记  → 对超过阈值的方法添加 @Ignore
7. 验证  → 重新构建并全量执行，确认主干稳定
```

## 关键设计决策

| 决策点 | 选择 | 原因 |
|--------|------|------|
| 编译环境 | Docker 内 JDK 8 | 本地 JDK 版本不一致，容器内保证一致性 |
| 镜像分发 | 先传一台再内网中转 | 内网带宽远大于公网 |
| 测试隔离 | 每轮独立容器 | 避免进程残留、端口占用等干扰 |
| 失败判定 | ≥1% 失败率 | 1000 次有效执行下 1% 约 10 次失败，平衡误判与漏判 |
| 标记方式 | `@Ignore` + 失败率注释 | 最小侵入，方便后续逐个启用 |
| 验证循环 | 标记后重新全量跑 | 处理"隐藏 flaky"问题 |

## 后续计划

- 对高失败率方法（>10%）优先根因分析并修复，修复后移除 `@Ignore` 并重新验证
- 考虑将检测工具集成到定期 CI 任务中，持续监控测试稳定性
