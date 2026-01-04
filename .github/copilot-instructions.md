## 目标
为 AI 编码代理提供可立即上手的、与本代码库紧密相关的上下文与约束，帮助快速修改、调试或添加功能。

## 大体架构（必读）
- **用途**: 这是一个用纯 Java 实现的调度器/模拟器项目，用来比较 Monolithic / Mesos / Omega 三类调度器的行为。
- **核心模块**: `src/main/java/scheduler/`（调度器实现：`IScheduler.java`, `BaseScheduler.java`, `MonolithicScheduler.java`, `MesosScheduler.java`, `OmegaScheduler.java`, `MesosAllocator.java`, `Offer.java`）和 `src/main/java/simulator/`（仿真器：`ClusterSimulator.java`, `MesosSimulator.java`, `OmegaSimulator.java`, `Simulator.java`）。
- **数据流与关键抽象**: 作业由 `Workload`/`Job` 表示，调度器调用 `scheduleJob` 返回 `ClaimDelta` 列表，之后由 `CellState.commit`/`unApply`/`scheduleEndEvents` 管理资源生命周期。请遵守 `ClaimDelta.apply/unApply/commit` 的语义（见 `simulator/core`）。

## 关键设计决策（代理必须知道）
- `BaseScheduler.scheduleJob` 使用随机 first-fit 算法；子类在队列与竞态控制上不同——不要改变 `IScheduler` 的公共签名。
- `MesosAllocator` 使用 DRF 选择被通知的调度器并构建 `Offer`（见 `MesosAllocator.java`）；资源申请/释放通过 `offeredDeltas` 管理，响应由 `respondToOffer` 处理。修改调度器–分配器交互时请同时检查这两个类。
- `OmegaScheduler`（参见 `OmegaScheduler.java`）采用乐观并发/事务提交模型：本地副本调度 + 提交/回滚，故更容易出现冲突，需要关注 `CellState.commit` 的返回值。

## 开发 / 运行 / 调试 快速命令
- 构建（Maven）:
```bash
mvn package
``` 
- 跳过测试构建:
```bash
mvn -DskipTests package
```
- 运行实验（项目根）:
```powershell
# Windows
.\experiments\run_all.ps1

# 或类 Unix
./experiments/run_all.sh
```
- 这些脚本假定存在 `experiments.ExperimentRunner` main 类并将输出写到 `experiments/results.csv`。

## 项目特定惯例与模式
- 调度器的 think-time 由两个映射控制：构造时传入的 `constantThinkTimes` 和 `perTaskThinkTimes`（见 `BaseScheduler` 构造签名）。新增工作负载时需在这两张表中添加条目。
- `ClusterSimulator` 在构造时将 `CellState` 和每个 `BaseScheduler` 的 `simulator` 字段设置好：在测试/单元运行时请通过 `setSimulator` 或在构造时注入来保证注册完整性。
- 统计变量（例如 `numSuccessfulTransactions` 等）位于 `BaseScheduler`，测试/分析代码通常直接读取这些字段；保持命名与语义一致。

## 代码改动注意事项
- 修改调度/资源申领流（`scheduleJob` / `ClaimDelta` / `CellState`）时：同时运行示例/实验并验证 `experiments/results.csv` 输出是否保持合理，避免引入不一致的资源泄漏。
- 不要随意改变 `IScheduler` 的 API；如果需要扩展，添加新接口或默认方法并更新 `ClusterSimulator` 的注册逻辑。

## 常用定位点（示例）
- 查看随机 first-fit 实现：`src/main/java/scheduler/BaseScheduler.java`
- Monolithic 调度器队列与重试逻辑：`src/main/java/scheduler/MonolithicScheduler.java`
- Mesos 报价/响应逻辑：`src/main/java/scheduler/MesosAllocator.java` 和 `src/main/java/scheduler/MesosScheduler.java`
- 仿真器/作业注入：`src/main/java/simulator/ClusterSimulator.java`

## 需要人工决策/不可自动化的地方
- 对 `CellState.commit` 冲突策略的调整需要设计决策（回滚、退避、全部或部分提交），AI 不应自动更改此处策略。
- 实验参数（如机器数量、offer 大小、think-time 等）在 README 示例或实验脚本中配置，修改这些数值会直接影响结果，应由开发者确认。

如果这些点有遗漏或你想把更多示例输出/实验用例加入到文件中，请告诉我我要补哪些部分。
