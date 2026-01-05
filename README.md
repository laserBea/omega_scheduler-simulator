# Scheduler Package

这个包包含了三个集群调度器的纯Java实现，完全不依赖Scala代码。

## 项目结构

### scheduler/ - 调度器包
包含所有调度器相关的实现：

```
scheduler/
├── IScheduler.java                # 调度器接口
├── BaseScheduler.java             # 调度器基类
├── MonolithicScheduler.java       # 单体调度器实现
├── MesosScheduler.java           # Mesos调度器实现
├── MesosAllocator.java           # Mesos资源分配器
├── Offer.java                    # 资源提供类
└── OmegaScheduler.java           # Omega调度器实现

```
## 三个调度器实现

### 1. MonolithicScheduler（单体调度器）
- **特点**: 使用简单的FIFO队列，直接访问共享的CellState
- **适用场景**: 单调度器场景
- **调度逻辑**:
  1. 作业添加到待处理队列
  2. 当调度器空闲时，从队列取出下一个作业
  3. 经过思考时间后，将作业调度到可用资源上
  4. 如果未完全调度，将作业重新入队

### 2. MesosScheduler（Mesos调度器）
- **特点**: 使用资源提供(Offer)机制，通过MesosAllocator管理资源分配
- **适用场景**: 多框架调度器场景
- **核心组件**:
  - `MesosScheduler`: 接收资源提供并调度作业
  - `MesosAllocator`: 使用DRF算法分配资源给调度器
- **调度逻辑**:
  1. 作业到达时，向分配器请求资源提供
  2. 收到提供后，尝试将队列中的作业调度到提供的资源上
  3. 响应提供，接受或拒绝资源
  4. 如果队列为空，取消未完成的提供请求

### 3. OmegaScheduler（Omega调度器）
- **特点**: 使用乐观并发控制，每个调度器维护私有CellState副本
- **适用场景**: 多个独立调度器并发访问共享集群
- **调度逻辑**:
  1. 同步公共CellState，获取私有副本
  2. 在私有副本上调度作业
  3. 提交事务以在公共CellState上声明资源
  4. 如果发生冲突，回滚并重试
  5. 如果作业未完全调度，重新入队

## 使用示例

```java
import simulator.core.CellState;
import simulator.core.Workload;
import simulator.ClusterSimulator;
import scheduler.IScheduler;
import scheduler.MonolithicScheduler;

// 创建CellState
CellState cellState = new CellState(
    10000,  // numMachines
    4.0,    // cpusPerMachine
    16.0,   // memPerMachine
    "resource-fit",      // conflictMode
    "all-or-nothing"     // transactionMode
);

// 创建调度器
Map<String, Double> constantThinkTimes = new HashMap<>();
constantThinkTimes.put("Batch", 0.01);
Map<String, Double> perTaskThinkTimes = new HashMap<>();
perTaskThinkTimes.put("Batch", 0.005);

MonolithicScheduler scheduler = new MonolithicScheduler(
    "Monolithic",
    constantThinkTimes,
    perTaskThinkTimes,
    0  // numMachinesToBlackList
);

// 创建模拟器
Map<String, IScheduler> schedulers = new HashMap<>();
schedulers.put("Monolithic", scheduler);

Map<String, List<String>> workloadMap = new HashMap<>();
workloadMap.put("Batch", Arrays.asList("Monolithic"));

List<Workload> workloads = new ArrayList<>();
List<Workload> prefillWorkloads = new ArrayList<>();

ClusterSimulator simulator = new ClusterSimulator(
    cellState,
    schedulers,
    workloadMap,
    workloads,
    prefillWorkloads,
    false  // logging
);

// 运行模拟
simulator.run(86400.0, null);  // 运行1天
```



## 运行环境
JDK
Apache Maven

## 最新测试结果统计

以下是三种调度器的测试结果统计表格，测试条件为是每个调度器10个实例，共300个任务。

| 调度器类型 | 成功事务数 | 重试事务数 | 总调度时间 (ms) |
|------------|------------|------------|-----------------|
| Monolithic | 597        | 0          | 1223.900        |
| Mesos      | 478        | 0          | 1073.700        |
| Omega      | 597        | 297        | 1223.900        |

根据测试结果可以得到如下结论：
- Monolithic 和 Omega 调度器的成功事务数相同，均为597
- Mesos 调度器的成功事务数较少，这是因为其资源提供机制在当前测试场景下的效率较低
- Mesos 的总调度时间较短，为1073.700ms
- Monolithic 和 Omega 的总调度时间相同，均为1223.900ms
Omega 调度器有297次重试事务，这是其乐观并发控制机制的结果，也是整个Omega论文所提到的机制。
这是因为Omega有多个调度器，调度器共享使用所有的集群状态信息，而且能根据掌握的集群资源信息进行资源分配，来决定调度的最终结果，共享状态方法的性能可行性最终取决于事务失败的频率和这种失败的代价。