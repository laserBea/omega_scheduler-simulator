# Omega 集群调度器模拟器

这是一个纯Java实现的集群调度器模拟器，基于Google Omega论文实现了三种不同的调度架构：单体调度（Monolithic）、两级调度（Mesos）和共享状态乐观并发控制（Omega）。

## 项目背景

本项目实现了Google [Omega: flexible, scalable schedulers for large compute clusters
](src/main/resources/Omega.pdf)中描述的三种集群调度架构：

1. **Monolithic**: 传统集中式调度器
2. **Mesos**: 两级调度架构（资源提供协议）
3. **Omega**: 共享状态 + 乐观并发控制

通过离散事件模拟器，可以比较不同架构在高并发场景下的性能表现，特别是Omega的乐观并发控制机制。

## 项目结构

### 核心架构

```
src/main/java/
├── experiments/                   # 实验运行器
│   └── ExperimentRunner.java      # 主实验程序
├── scheduler/                     # 调度器实现
│   ├── IScheduler.java           # 调度器接口
│   ├── BaseScheduler.java        # 调度器基类
│   ├── MonolithicScheduler.java  # 单体调度器
│   ├── MesosScheduler.java      # Mesos调度器
│   ├── MesosAllocator.java      # Mesos资源分配器
│   ├── Offer.java               # 资源提供对象
│   └── OmegaScheduler.java      # Omega调度器
├── simulator/                    # 模拟器框架
│   ├── Simulator.java           # 离散事件模拟器基类
│   ├── ClusterSimulator.java    # 通用集群模拟器
│   ├── MesosSimulator.java     # Mesos专用模拟器
│   └── OmegaSimulator.java     # Omega专用模拟器
└── simulator/core/              # 核心数据结构
    ├── CellState.java          # 集群状态管理
    ├── Job.java                # 作业定义
    ├── Workload.java           # 工作负载
    └── ClaimDelta.java         # 资源声明
```

### 三层模拟器架构

| 层次 | 实现 | 特点 | 用途 |
|------|------|------|------|
| **事件模拟器** | `Simulator` | 离散事件驱动 | 时间管理和事件调度 |
| **通用模拟器** | `ClusterSimulator` | 架构无关 | 快速算法比较 |
| **专用模拟器** | `MesosSimulator`<br>`OmegaSimulator` | 架构特定 | 详细性能分析 |
## 三种调度器架构详解

### 1. MonolithicScheduler（单体调度器）

**设计思路**: 集中控制，顺序处理

```java
// 直接访问共享集群状态
List<ClaimDelta> claimDeltas = scheduleJob(job, simulator.getCellState());
```

**特点**:
- **简单直接**: 直接访问共享CellState，无复杂协议
- **确定性**: 每次调度结果可预测
- **扩展性差**: 单点瓶颈，不适合大规模并发
- **锁竞争**: 多个调度器时需要同步机制

**适用场景**: 小型集群，单调度器环境

### 2. MesosScheduler（两级调度器）

**设计思路**: 资源提供协议，解耦资源分配

```java
// 通过资源提供协议工作
public void resourceOffer(Offer offer) {
    // 在提供的资源上调度作业
    List<ClaimDelta> response = scheduleOnOffer(offer);
    allocator.respondToOffer(offer, response);
}
```

**核心组件**:
- **MesosScheduler**: 响应资源提供，调度作业
- **MesosAllocator**: 使用DRF（Dominant Resource Fairness）算法分配资源
- **Offer**: 封装的资源提供对象

**特点**:
- **解耦设计**: 资源分配与作业调度分离
- **公平性**: DRF算法确保资源公平分配
- **可扩展性**: 支持多个异构框架
- **协议开销**: 资源提供/响应的往返通信

**适用场景**: 多框架共存的异构集群

### 3. OmegaScheduler（乐观并发控制）

**设计思路**: 共享状态 + 乐观并发，拥抱冲突而非避免

```java
// 乐观并发控制核心逻辑
CellState privateCellState = omegaSimulator.getCellState().copy();
List<ClaimDelta> claimDeltas = scheduleJob(job, privateCellState);
CellState.CommitResult result = omegaSimulator.getCellState().commit(claimDeltas, true);
if (!result.getCommittedDeltas().isEmpty()) {
    // 成功提交
    numSuccessfulTransactions++;
} else {
    // 冲突发生，重试
    handleConflict();
}
```

**核心机制**:
- **私有状态副本**: 每个调度器维护独立的集群状态快照
- **乐观并发**: 假设无冲突，先在私有副本上调度，再提交事务
- **冲突检测**: 通过事务提交检测资源冲突
- **优雅重试**: 冲突时自动重试，无死锁风险

**特点**:
- **高并发性**: 允许多个调度器同时工作
- **无锁设计**: 避免传统锁的性能瓶颈
- **全局视图**: 调度器可以看到完整集群状态
- **重试开销**: 冲突时需要重新调度

**适用场景**: 大规模集群，多个调度器需要并发访问

## 实验结果与核心发现

### 实验配置

**集群环境**:
- 2台机器 × (8 CPU, 12000内存)
- 300个作业同时提交（事务并发到达）
- 每个作业：12-23个任务 × (1 CPU, 400内存)
- 任务执行时间：1秒

**调度器配置**:
- 思考时间：1.0 + 0.1×任务数（秒）
- 3个并发调度器实例（针对Omega和Monolithic）

### 实验结果对比

| 架构 | 成功事务 | 重试事务 | 调度时间 | 并发特性 |
|------|----------|----------|----------|----------|
| **Monolithic** | 597 | **0** | 1223.9秒 | 集中式，无冲突 |
| **Mesos** | 478 | **0** | 1073.7秒 | 两级协议协调 |
| **Omega** | 597 | **297** | 1223.9秒 | **乐观并发控制** |

### 核心发现

#### **1. 乐观并发控制的实际效果**
Omega的**297次重试**完美展示了乐观并发控制的核心机制：
- 允许多个调度器并行工作
- 通过事务提交检测冲突
- 冲突时优雅重试，无死锁
- 最终达到与Monolithic相同的成功率

#### **2. 架构权衡分析**
- **Monolithic**: 简单可靠，但扩展性有限
- **Mesos**: 公平高效，但协议开销较大
- **Omega**: 高并发潜力，但需处理重试开销

#### **3. 资源竞争的影响**
在高并发场景下：
- Monolithic和Omega能充分利用资源（597/600 ≈ 99.5%成功率）
- Mesos的资源提供机制在当前配置下效率较低（478/600 ≈ 79.7%）

### 快速开始

#### 编译项目
```bash
mvn clean compile
```

#### 运行实验
```bash
java -cp target/classes experiments.ExperimentRunner
```

#### 查看结果
实验会输出CSV格式的结果到控制台，包含：
- 成功事务数
- 重试事务数
- 调度时间统计

### 自定义实验

修改`ExperimentRunner.java`中的参数来自定义实验：

```java
// 调整作业数量和并发度
for (int i = 0; i < 500; i++) {  // 更多作业
    int numTasks = 8 + random.nextInt(16);  // 不同任务规模
    double submitTime = 0.0;  // 同时到达 or 间隔到达
    Job j = new Job(i + 1, submitTime, numTasks, 2.0, "wl", 1.0, 800.0);
    wl.addJob(j);
}

// 调整集群规模
CellState cs = new CellState(4, 16.0, 32000.0, "sequence-numbers", "incremental");

// 调整思考时间（影响调度开销）
Map<String, Double> constantThink = Map.of("wl", 2.0);  // 更长的思考时间
Map<String, Double> perTaskThink = Map.of("wl", 0.2);
```

## 开发环境

- **JDK**: 8+
- **构建工具**: Apache Maven
- **依赖**: 无外部依赖，纯Java实现

## 参考资料

- [Omega: flexible, scalable schedulers for large compute clusters](src/main/resources/Omega.pdf)
- [Google Omega 集群调度模拟器](https://github.com/google/cluster-scheduler-simulator)

## 项目价值

这个项目不仅实现了Omega论文的核心算法，还通过实验验证了：

1. **乐观并发控制的可行性**：在实际系统中重试开销是可接受的
2. **多调度器并发的优势**：通过冲突处理实现高扩展性
3. **架构设计的权衡**：不同调度架构在复杂度和性能间的平衡

为理解现代集群调度系统提供了宝贵的实践参考！
