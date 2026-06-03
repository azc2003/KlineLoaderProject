# KlineLoader 性能实验报告

> 完整记录所有性能优化实验的数据、方法与结论。
> 测试环境：EC2-1 Producer (t2.micro, 1 vCPU) | EC2-2 Kafka (Docker) | EC2-3 Consumer (t2.micro, 1 vCPU) | RDS PostgreSQL (db.t3.micro, 1 vCPU/1GB) | Region us-east（EC2）/ us-east-2（RDS）
> 基准数据集：BTCUSDT 1 个月 1 分钟 K 线 = **44,640 条**（Binance klines 接口单次上限 1000，需 45 次调用）

---

## 实验总览

| # | 实验 | 结论一句话 | 提升 |
|---|------|-----------|------|
| 1 | Binance Fetch 三种并发模式 | I/O 密集任务用自定义线程池，不要用并行流 | 串行 1736ms → 线程池 315ms (**5.5x**) |
| 2 | Kafka Producer 发送方式 | 串行 for → CompletableFuture 异步并行 | ~1.7s → ~0.3s (**~5x**) |
| 3 | Consumer 单条 vs Batch Listener | 一次 poll 拿一批，batchInsert 一条 SQL | 单条 → 批量，去掉 scheduler |
| 4 | max.poll.records 扫描 (250→7000) | 收益递减，真瓶颈是 RDS 写入 | 28 倍批大小只换 21% |
| 5 | INSERT vs COPY | COPY 到临时表 + ON CONFLICT 转入 | 写入 5s → 2s (**2.5x**) |

---

## 实验 1：Binance Fetch 三种并发模式 ⭐

### 方法
- 接口 `POST /klineloader?...&mode=serial|parallel|pool`
- 只计时 fetch 阶段（不含 Kafka 发送）
- 1 个月数据 = 45 次 Binance API 调用，每种模式跑 3 次取最佳

### 三种模式
| 模式 | 实现 | 线程来源 |
|------|------|---------|
| serial | 串行 `for` 循环 | 单线程 |
| parallel | `IntStream.range().parallel()` | 公共 ForkJoinPool（并行度 = CPU核数-1）|
| pool | `Executors.newFixedThreadPool(20)` | 自定义 20 线程 |

### 数据
| 模式 | Run1 | Run2 | Run3 | **最佳** | 相对串行 |
|------|------|------|------|---------|---------|
| serial | 2330ms | 1867ms | 1736ms | **1736ms** | 1.0x |
| parallel | 908ms | 825ms | 727ms | **727ms** | **2.4x** |
| **pool** | 500ms | 327ms | 315ms | **315ms** | **5.5x** 🏆 |

### 结论
- **parallel stream 在 1 vCPU 上几乎退化成串行**：ForkJoinPool 公共池并行度 = `availableProcessors() - 1 = 0 → 兜底 1`，只有 ~1-2 线程
- **fetch 是 I/O 密集**（90% 时间等网络），CPU 在等待时空闲，1 个核也能调度 20 个 I/O 线程
- **判断口诀**：任务在"算"→并行流（按 CPU 数）；任务在"等"→自定义线程池（开很多线程）
- 线程数 20 是经验值，再大受 Binance rate limit 和单段延迟限制，收益递减

### 1 vCPU 时间轴
```
串行：  [等网络][CPU][等网络][CPU]...  CPU 大量空闲   1736ms
并行流：[等][等] 只有 1-2 线程         CPU 仍浪费     727ms
线程池：[等]×20 20请求同时等          接近网络极限   315ms
```

---

## 实验 2：Kafka Producer 发送方式

### 方法
1 个月 44,640 条发送到 Kafka，对比串行 vs 异步并行。

### 实现对比
```java
// 优化前：串行 for，每条等 ACK
for (Kline k : klines) kafkaTemplate.send(TOPIC, k.getSymbol(), k);

// 优化后：全部异步发出，最后统一等
List<CompletableFuture<?>> futures = klines.stream()
    .map(k -> kafkaTemplate.send(TOPIC, k.getSymbol(), k).toCompletableFuture())
    .collect(toList());
CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
```

### 数据
| 方式 | 耗时 |
|------|------|
| 串行 for | ~1.7s |
| CompletableFuture 异步并行 | ~0.3s |

### 结论
- 串行瓶颈在"每条等 Kafka ACK 再发下一条"
- 异步并行一次性发出，最后 join 等全部完成，**~5x**

---

## 实验 3：Consumer 单条 vs Batch Listener

### 方法
对比单条 `@KafkaListener(Kline)` + 内存 buffer 攒批 vs `setBatchListener(true)` 一次 poll 拿一批。

### 实现对比
```java
// 优化前：单条触发，内存 buffer 攒够 5000 才写，最后一批靠 @Scheduled flush
public void consume(Kline kline, Acknowledgment ack) { buffer.add(kline); ... }

// 优化后：Batch Listener，一次 poll 拿 List，直接 batchInsert
public void consumeBatch(List<Kline> klines, Acknowledgment ack) {
    klineMapper.batchInsert(klines);
    ack.acknowledge();
}
```

### 结论
- 单条版本问题：44,640 次方法调用 + 最后一批 <5000 永远不写（靠 scheduler 兜底 +5s 延迟）
- Batch Listener：Kafka 自然攒批，去掉 buffer 和 scheduler，最后 flush 等待 5s → 0s
- 配置：`MAX_POLL_RECORDS=2000`, `FETCH_MIN_BYTES=100KB`, `FETCH_MAX_WAIT_MS=500`

---

## 实验 4：max.poll.records 扫描 ⭐

### 方法（隔离测量）
1. 删除并重建 `kline-topic`，Producer 预灌恰好 44,640 条（Consumer 停机时灌入，消息堆积）
2. 每个 max.poll.records 值用**独立 group + auto.offset.reset=earliest** 从头消费全部 44,640 条
3. 测量"写入窗口"（首批日志时间 → 末批日志时间），隔离 Consumer 纯写入性能

### 数据
| max.poll.records | 批次数 | 写入窗口(首→末) | SQL累计 | 最慢单批 | 平均吞吐 |
|------------------|--------|----------------|---------|---------|---------|
| 250 | 182 | 5738ms | 5825ms | 1247ms | 7780 rec/s |
| 500 | 93 | 5574ms | 5769ms | 1368ms | 8010 rec/s |
| 1000 | 48 | 5441ms | 5906ms | 1465ms | 8204 rec/s |
| 2000 | 27 | 5578ms | 6390ms | 1778ms | 8003 rec/s |
| **4000** | 18 | **4862ms** | 6140ms | 2206ms | **9181 rec/s** |
| 7000 | 9 | 4509ms | 6542ms | 2953ms | 9900 rec/s |

### 结论（反直觉）
- **批大小调 28 倍（250→7000），写入只快 21%**（5738→4509ms）
- **SQL 累计耗时几乎恒定 ~6 秒** → 真瓶颈是 RDS db.t3.micro 写入能力（~7400-8000 rec/s），不是批处理开销
- 批越大尾延迟越糟：最慢单批 1247ms → 2953ms，内存峰值 + 失败重试代价上升
- **拐点 = 4000**：2000→4000 改善明显(-13%)，4000→7000 收益递减(-7%) 且尾延迟翻倍
- 启示：**继续调这个参数是"优化错了地方"**，真正提速要换方向（COPY / 升级 RDS / 多 partition 并行）

### PostgreSQL 参数上限约束
- 单条 SQL 参数上限 ~65,535；每行 8 字段 → 行数上限 ~8000
- 实验最大取 7000（7000×8=56000，安全）

---

## 实验 5：INSERT vs COPY ⭐⭐

### 方法
同实验 4 隔离方式（预灌 44,640 条，独立 group 从头读），固定 max.poll.records=4000，对比 INSERT 与 COPY 两种写入模式，各跑 2 次。

### 实现
```java
// INSERT 模式：MyBatis 多行 VALUES + ON CONFLICT DO NOTHING（4000 行/条 SQL）

// COPY 模式（KlineCopyWriter）：
// 1. COPY 到临时表 kline_staging（无约束，极快）
CopyManager cm = conn.unwrap(PGConnection.class).getCopyAPI();
cm.copyIn("COPY kline_staging FROM STDIN WITH (FORMAT csv)", new StringReader(csv));
// 2. 转入主表，保留幂等
"INSERT INTO kline SELECT * FROM kline_staging ON CONFLICT (symbol,open_time,close_time) DO NOTHING"
```

### 数据
| 模式 | 批次数 | 写入窗口 | SQL累计 | 最慢单批 |
|------|--------|---------|---------|---------|
| INSERT (run1) | 18 | 5112ms | 6477ms | 2211ms |
| INSERT (run2) | 18 | 4930ms | 6317ms | 2299ms |
| COPY (run1) | 18 | 2003ms | 2028ms | 1101ms |
| COPY (run2) | 18 | 2082ms | 2026ms | 1010ms |
| **平均对比** | — | **5021 → 2042ms** | **6397 → 2027ms** | **2255 → 1055ms** |
| **提升** | — | **2.5x** | **3.1x** | 2.1x |

### 结论
- **COPY 写入提速 2.5x**，DB 写入从 ~5s 降到 ~2s，不再是绝对瓶颈
- COPY 绕过逐行 SQL 解析/类型检查，是 PG 官方推荐的大批量导入方式
- **关键技巧**：COPY 不支持 ON CONFLICT → 先 COPY 到临时表，再 INSERT...SELECT...ON CONFLICT 转入主表，**速度与幂等兼得**
- 比死磕 max.poll.records（28 倍→21%）有效得多，因为**找对了瓶颈**

### 端到端预期
```
INSERT：fetch 0.88s + send 0.74s + 写入 ~5s ≈ 端到端 4.3s
COPY：  fetch 0.88s + send 0.74s + 写入 ~2s ≈ 端到端 3.0s
```

---

## 全链路 Latency 拆解（1 个月 44,640 条）

| 阶段 | 优化前 | 优化后 | 手段 |
|------|--------|--------|------|
| Binance API fetch | 1736ms（串行）| **315ms** | 自定义 20 线程池（实验1）|
| Kafka 发送 | ~1700ms | ~300ms | CompletableFuture 异步并行（实验2）|
| Kafka→Consumer 传输 | <200ms | <200ms | 近实时 |
| Consumer 写库 | ~5000ms（INSERT）| **~2000ms** | COPY 临时表（实验5）|
| 最后 flush 等待 | 5000ms（scheduler）| 0ms | Batch Listener 自然攒批（实验3）|
| **端到端** | **~11s** | **~3s** | — |

---

## v2 新增功能验证（realtime / 双Listener / gap补偿 / 监控团队）

### 验证 A：双 Listener 并发不阻塞（实测线程隔离）
触发 backfill（44640条灌 kline-backfill）同时观察 realtime：

| 时间 | 来源 | 线程名 | 批量 |
|------|------|--------|------|
| 00:27:31~33 | [BACKFILL] | `container#1-0-C-1` | 74批×500-1475条 |
| 00:28:00 | [REALTIME] | `container#0-0-C-1` | 2-3条 / 12ms |

**结论**：两个 Listener 跑在不同线程（#1 / #0），backfill 灌 4 万条期间 realtime 准时落库，互不阻塞。realtime 低延迟配置生效（来几条立刻写 12ms）。

### 验证 B：realtime+backfill 重叠幂等（实测 0 重复）
realtime 先写 BTCUSDT 某分钟，backfill 补覆盖同窗口：
- 重复行检测：**0 rows**
- backfill 日志：`inserted 29/30 | skipped 1` ← 重叠那条被 UPSERT 跳过
- 数据质量：时间0缺口、OHLCV 0异常

### 验证 C：查询缺口自动补偿（实测 1.8s 补回）
删除中间一条(open_time=1780446480000)后查询 `/klineAggregate`：
```
01:07:08.073  [GAP] 1 gap(s) → triggering backfill
01:07:09.861  [GAP] fill result 29/29  ✅（1.8s）
→ gap_filled=t（被删数据补回），返回6桶 volume守恒4.7096
```

### 验证 D：volume double-count bug（修复前后对比）
原始29条总volume=4.70960，按不同interval聚合后总量应守恒：

| interval | 修复前 | 修复后 |
|----------|--------|--------|
| 5m | 4.70960 | 4.70960 |
| 7m (末桶size=1) | **4.71164** ❌ | 4.70960 ✅ |
| 10m | 4.70960 | 4.70960 |

**根因**：aggregate() 在 size==1 时 `first==last`，volume 被加两次。修复：改为每元素遍历一次。

### 验证 E：多 Consumer Group 扇出（Kafka 核心价值）
`kafka-consumer-groups --describe`：

| Group | Topic | Offset | LAG |
|-------|-------|--------|-----|
| kline-monitor-group (风控) | kline-realtime | 234 | 0 |
| kline-db-writer-group-realtime (存储) | kline-realtime | 234 | 0 |

同一份数据被两个独立 group 消费，各自 offset，LAG=0。监控团队实时告警实测：
```
SOLUSDT ⚠️ RISK ALERT（1分钟波动超0.1%阈值）
BTCUSDT ok (0.068%) / ETHUSDT ok (0.079%)
```
**加监控团队零改动现有写库服务** —— 这是 Kafka 相比共享DB/HTTP推送的核心优势。

---

## 仍可做的优化（按影响排序）

### 🔴 v2 引入的待修隐患（gap 补偿的配套保护）
| 问题 | 风险 | 状态 |
|------|------|------|
| gap-fill 阻塞 Tomcat 线程(Thread.sleep≤15s) | 并发查询耗尽线程/连接池 | 未做 |
| gap-fill 死循环（补不上的分钟反复触发） | 毒查询每次慢15s | 未做 |
| Producer 无连接池配置（HikariCP默认10）| gap-fill 长持连易耗尽 | 未做 |

### 🟡 架构/可靠性
| 优化 | 影响 | 状态 |
|------|------|------|
| Kafka RF=1→3 + 多 partition | 消除 SPOF + 可水平扩展 | 未做 |
| realtime 断线主动补偿（重连后补缺口）| 现仅查询时被动补 | 未做 |
| Consumer 死信队列(DLQ) | 毒消息防卡住 partition | 未做 |
| 接口改 202 立即返回 + /status | HTTP 阻塞 → 毫秒级 | 未做 |
| Producer RateLimiter | 防 Binance 限流（1200/分钟）| 未做 |

### 🟢 工程完整性
| 优化 | 影响 | 状态 |
|------|------|------|
| 集成测试(Testcontainers) | 几乎无测试（仅1空test）| 未做 |
| GitHub Actions CI/CD | 告别手动部署 | 未做 |
| 密码进 Secrets Manager | 现明文在 systemd env | 未做 |
| Kafka lag / CloudWatch 监控 | 无可观测性 | 未做 |

> 注：EC2 与 RDS 实际同在 us-east-2（SG 跨账号引用证明同 VPC），无跨区问题。

---

## 方法论沉淀（面试可讲）

1. **先定位再优化**：拆解全链路 latency，找到真瓶颈（DB 写入占 70%），别在非瓶颈上花力气
2. **区分 I/O 密集 vs CPU 密集**：fetch 用线程池不用并行流，这是实验 1 的核心洞察
3. **诚实的负面结果也有价值**：实验 4 证明"调 max.poll.records 是优化错了地方"，比假装有效更可信
4. **换方向比调参数有效**：实验 5 的 COPY 一招比实验 4 调 28 倍参数收益大得多
5. **优化不牺牲正确性**：COPY 用临时表中转保住了幂等去重
