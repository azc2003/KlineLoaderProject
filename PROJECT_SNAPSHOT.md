# KlineLoaderProject — 完整项目快照与文档

> 最后更新：2026-06-03（v2：新增 realtime / 双topic / 双Listener / gap补偿 / 监控团队 / systemd）
> 用途：分布式加密货币 K 线数据采集、传输、存储与查询系统
> 本文档用于：恢复上下文、面试讲解、项目交接
>
> ⚠️ **下方"系统架构"等章节为 v1（仅 backfill）。最新完整架构见本文件顶部的 [架构演进 v2](#架构演进-v2)。**

---

## 目录
1. [一句话概述](#一句话概述)
2. [系统架构](#系统架构)
3. [AWS 基础设施清单](#aws-基础设施清单)
4. [数据流向](#数据流向)
5. [项目结构与代码设计](#项目结构与代码设计)
6. [关键配置](#关键配置)
7. [部署与启动命令](#部署与启动命令)
8. [性能优化记录](#性能优化记录)
9. [已修复的 Bug](#已修复的-bug)
10. [恢复上下文用的快照](#恢复上下文用的快照)
11. [待办 / 下一步](#待办--下一步)

---

## 一句话概述

一个**多服务分布式 K 线数据系统**：从 Binance API 拉取历史 K 线（Backfill），通过 Kafka 解耦传输，由独立的 Consumer 服务批量写入 PostgreSQL，并提供多周期聚合查询 API。部署在 3 台 EC2 + 1 个 RDS 上，体现了**生产者/消费者解耦、故障隔离、水平扩展**的架构思想。

---

## 架构演进 v2

v1 只有 backfill 单链路。v2 增加了实时流、双 topic 隔离、双 listener、查询缺口补偿、下游监控团队、systemd 守护。

### v2 完整架构

```
Binance
  ├─ Backfill  (REST API 拉取)    ──→ kline-backfill ─┐
  └─ Realtime  (WebSocket 订阅)   ──→ kline-realtime ─┤
        (EC2-1 同一个 jar，systemd)                    ↓
                                                    Kafka (EC2-2)
                          ┌──────────────────────────┼─────────────────────────┐
                          ↓                          ↓                          ↓
              db-writer-group-backfill   db-writer-group-realtime      kline-monitor-group
              (Listener:backfillFactory) (Listener:realtimeFactory)    (风控团队,独立服务)
              高吞吐 max.poll=4000        低延迟 fetch.min=1            实时价格异常检测
                          ↓                          ↓                          ↓
                    COPY → RDS                COPY → RDS                  RISK ALERT 告警
                  (EC2-3, systemd)          (EC2-3, systemd)            (EC2-3, systemd)
                                                                              ↑ 不写库

   查询 GET /klineAggregate (EC2-1)
     → 读 RDS → 检测缺口 → 有缺口则触发 backfill 补齐 → 等待入库 → 聚合返回
```

### v2 新增组件清单

| 组件 | 位置 | 作用 |
|------|------|------|
| `BinanceRealtimeProducer` | KlineLoaderProject/service | WebSocket 订阅 binance.us，过滤收盘K线(x=true)，发 kline-realtime |
| `KlineProducer` (改) | KlineLoaderProject/service | 双 topic：sendAll→kline-backfill，sendRealtime→kline-realtime |
| `KlineAggregateService` (改) | KlineLoaderProject/service | 查询缺口检测 + 自动 backfill 补偿 + volume bug 修复 |
| `KafkaConfig` (改) | KlineConsumerProject/config | 双 Factory：realtimeFactory(低延迟) + backfillFactory(高吞吐) |
| `KlineConsumer` (改) | KlineConsumerProject/service | 双 Listener：consumeRealtime + consumeBackfill，独立线程 |
| `KlineMonitorProject` (新) | EC2-3 独立服务 | 风控团队：独立 group 消费同一份数据，价格异常告警，不写库 |
| 3 个 systemd service | /etc/systemd/system/ | kline-producer / kline-consumer / kline-monitor，守护+崩溃自启 |

### v2 关键设计

1. **双 Topic 物理隔离**：`kline-backfill`（吞吐型）+ `kline-realtime`（延迟型），backfill 爆发不污染 realtime 流。

2. **双 Listener 线程隔离**：Consumer 用两个独立 `@KafkaListener` + 两个 ContainerFactory + 两个 Consumer Group，各自独立线程。backfill 灌 4 万条时 realtime 仍准时落库（实测两线程 container#1 / container#0 并发）。
   - realtimeFactory：`fetch.min.bytes=1, max.wait=100ms, max.poll=500`（低延迟）
   - backfillFactory：`fetch.min.bytes=100KB, max.wait=500ms, max.poll=4000`（高吞吐）

3. **幂等去重（三层保障）**：realtime 和 backfill 可能写同一根 K 线 → PK + `INSERT ON CONFLICT DO NOTHING` 自动跳过。实测重叠场景 `skipped 1`，0 重复行。

4. **查询缺口自动补偿**：`/klineAggregate` 先检测 `[start,end]` 内是否缺分钟 → 缺则调 `loadService.loadURL` backfill 那段 → 轮询 DB 直到补齐(≤15s) → 再聚合返回。实测删中间一条后查询，1.8s 自动补回。
   - 副作用好处：补齐后数据连续，下标分桶不再有错位风险。

5. **多 Consumer Group 扇出（Kafka 核心价值实证）**：`kline-monitor-group`（风控）与 `db-writer-group-realtime`（存储）独立消费同一 `kline-realtime`，各自 offset，LAG=0。加监控团队**零改动现有写库服务**。

6. **systemd 守护**：解决 `nohup` over-ssh 被 SIGHUP 杀的问题。密码进 `/etc/*.env`（权限600），`Restart=always` 崩溃自启。

### v2 数据正确性验证（全部实测通过）

| 验证 | 结果 |
|------|------|
| realtime+backfill 重叠 | 0 重复行，UPSERT skipped 1 |
| 时间连续性 | 0 缺口（相邻=60000ms）|
| OHLCV 合理性 | 0 异常（high≥low, close∈[low,high]）|
| volume 体积守恒 | 各 interval 聚合总量一致(4.7096) |
| 缺口补偿 | 删1条→1.8s自动补回 |
| 多 group 独立消费 | monitor/writer 各自 offset |

---

## 系统架构

```
                          ┌─────────────────────┐
   POST /klineloader      │   EC2-1 Producer    │
   ───────────────────►   │   (kline-app-sg)    │
                          │   Spring Boot :8080 │
                          │                     │
                          │  Controller         │
                          │     ↓               │
                          │  ValidationService  │  ← 启动时缓存 Binance 全部 symbol
                          │     ↓               │
                          │  LoadService        │  ← 并行 fetch + 异步发 Kafka
                          │     ↓               │
                          │  BinanceAPIService  │  ← 调 Binance REST，并行解析
                          │     ↓               │
                          │  KlineProducer      │  ← 异步批量 send，symbol 作 key
                          └──────────┬──────────┘
                                     │ Kafka Producer
                                     ↓
                          ┌─────────────────────┐
                          │   EC2-2 Kafka       │
                          │   (kline-kafka-sg)  │
                          │   Docker:           │
                          │   - Zookeeper :2181 │
                          │   - Kafka :9092     │
                          │   Topic: kline-topic│
                          │   (symbol 作 key)   │
                          └──────────┬──────────┘
                                     │ Kafka Consumer
                                     ↓
                          ┌─────────────────────┐
                          │   EC2-3 Consumer    │
                          │   (kline-consumer-sg)│
                          │   Spring Boot       │
                          │                     │
                          │  KlineConsumer      │  ← Batch Listener
                          │     ↓               │
                          │  KlineMapper        │  ← batchInsert + UPSERT
                          └──────────┬──────────┘
                                     │ JDBC :5432
                                     ↓
                          ┌─────────────────────┐
                          │   RDS PostgreSQL    │
                          │   (kline-rds-sg)    │
                          │   db: klinedb       │
                          │   table: kline      │
                          └─────────────────────┘

   查询路径（同样在 EC2-1）：
   GET /klineAggregate ──► KlineAggregateController ──► KlineAggregateService
                            ──► KlineMapper.getRawKline (从 RDS 读)
                            ──► 并行聚合多个时间桶 ──► 返回多周期 K 线
```

---

## AWS 基础设施清单

### Region: `us-east-2` (Ohio) — RDS / `us-east-1`? 实际 RDS 在 us-east-2

> ⚠️ 注意：EC2 在创建时选了 us-east-1，但 RDS endpoint 显示 us-east-2。下次确认统一 Region。

### EC2 实例（全部 t2.micro，免费层）

| 名称 | 公网 IP | 私网 IP | Security Group | 安装内容 |
|------|---------|---------|----------------|----------|
| kline-producer | `3.15.15.144` | — | kline-app-sg | Java17, Maven, Git, Docker |
| kline-kafka | `18.216.59.173` | `172.31.2.188` | kline-kafka-sg | Docker, docker-compose, Kafka, Zookeeper |
| kline-consumer | `18.227.21.108` | — | kline-consumer-sg | Java17, Maven, Git, psql |

### RDS

| 项 | 值 |
|----|----|
| Instance ID | kline-db |
| Engine | PostgreSQL 15 |
| Endpoint | `<RDS_ENDPOINT>.us-east-2.rds.amazonaws.com` |
| Port | 5432 |
| Database | klinedb |
| Username | postgres |
| Password | `<YOUR_DB_PASSWORD>` （⚠️ 仅学习用，生产应进 Secrets Manager）|
| Instance Class | db.t3.micro（免费层）|
| Public Access | No |

### Security Groups（最小权限设计）

| SG | Inbound 规则 |
|----|--------------|
| **kline-app-sg** | SSH 22 ← My IP；TCP 8080 ← 0.0.0.0/0 |
| **kline-kafka-sg** | SSH 22 ← My IP；TCP 9092 ← kline-app-sg + kline-consumer-sg |
| **kline-consumer-sg** | SSH 22 ← My IP |
| **kline-rds-sg** | PostgreSQL 5432 ← kline-consumer-sg (+ app-sg 如需查询) |

> 设计要点：用 **SG 引用 SG**（而非 IP），新增机器挂同一个 SG 即自动获得权限，IP 变了也不用改规则。

### Key Pair
- 名称：`kline-key`
- 本地路径：`~/Downloads/kline-key.pem`（三台 EC2 共用）
- 使用：`ssh -i ~/Downloads/kline-key.pem ec2-user@<IP>`

### 成本控制
- 三台 t2.micro 共享 750 小时/月免费额度
- **每天用完务必 Stop（不是 Terminate）**，每天用 ≤8 小时 = 完全免费
- 已建议设置 Zero Spend Budget 告警

---

## 数据流向

### Backfill 写入路径（一根 K 线的旅程）

```
1. curl POST /klineloader?symbol=BTCUSDT&startTime=...&endTime=...
2. KlineLoadController 接收，调 ValidationService 校验 symbol 白名单
3. LoadService 按 limit(1000) 把时间范围切成 N 段
4. IntStream.parallel() 并行调 Binance REST（约 10 路并发）
5. BinanceAPIService 把每段 JSON 并行解析成 List<Kline>
6. 全部按 openTime 排序
7. KlineProducer.sendAll() 异步并行发到 Kafka（symbol 作 key）
8. Kafka kline-topic 持久化，按 symbol hash 进 partition
9. EC2-3 KlineConsumer 批量 poll（最多 2000 条/次）
10. KlineMapper.batchInsert() → INSERT ON CONFLICT DO NOTHING（幂等去重）
11. 写库成功后 ack.acknowledge() 提交 offset
12. RDS klinedb.kline 落盘
```

### 查询路径

```
1. GET /klineAggregate?symbol=BTCUSDT&interval=1h&startTime=...&endTime=...
2. KlineAggregateController 校验
3. KlineAggregateService.get() 从 RDS 读 1 分钟原始 K 线
4. 按 interval 切成时间桶，IntStream.parallel() 并行聚合
5. 每个桶算出 open/high/low/close/volume
6. 返回聚合后的多周期 K 线列表
```

---

## 项目结构与代码设计

### 项目 1：KlineLoaderProject (Producer + Query) — 部署在 EC2-1

```
KlineLoaderProject/
├── pom.xml                          # Spring Boot 3.5.0, Java 17
├── src/main/java/com/example/demo/
│   ├── DemoApplication.java         # 启动类
│   ├── config/
│   │   ├── AppConfig.java           # RestTemplate Bean
│   │   └── LogAspect.java           # @Around 切面，记录聚合方法耗时
│   ├── controller/
│   │   ├── KlineLoadController.java       # POST /klineloader（触发 Backfill）
│   │   ├── KlineAggregateController.java   # GET /klineAggregate（查询聚合）
│   │   └── ControllerExceptionHandler.java # 全局异常 → 统一 400
│   ├── model/
│   │   ├── Kline.java               # symbol/openTime/closeTime/OHLCV
│   │   ├── IntervalUnit.java        # 枚举：m=1, h=60, d=1440 分钟
│   │   └── ex/InputException.java   # 自定义输入异常
│   ├── repo/
│   │   └── KlineMapper.java         # MyBatis：getRawKline（读）+ batchInsert（写，已不用）
│   └── service/
│       ├── BinanceAPIService.java   # 调 Binance REST，并行解析 JSON
│       ├── GetAllSymbolService.java # @PostConstruct 缓存全部合法 symbol
│       ├── ValidationService.java   # 参数校验（symbol 白名单 + interval 格式）
│       ├── LoadService.java         # 核心：并行 fetch → 异步发 Kafka
│       ├── KlineProducer.java       # 异步批量 send 到 Kafka
│       └── KlineAggregateService.java # 并行聚合多个时间桶
└── src/main/resources/
    └── application.properties       # RDS + Kafka Producer 配置
```

### 项目 2：KlineConsumerProject (Consumer) — 部署在 EC2-3

```
KlineConsumerProject/
├── pom.xml                          # Spring Boot 3.5.0, web+kafka+mybatis
└── src/main/java/com/example/consumer/
    ├── ConsumerApplication.java     # 启动类
    ├── config/
    │   └── KafkaConfig.java         # Batch Listener Factory + Consumer 调优
    ├── model/
    │   └── Kline.java               # 与 Producer 字段一致（独立包名）
    ├── repo/
    │   └── KlineMapper.java         # batchInsert + ON CONFLICT DO NOTHING
    └── service/
        └── KlineConsumer.java       # Batch Listener，批量写库 + 手动 ack
```

### 核心设计要点

| 设计 | 说明 | 为什么 |
|------|------|--------|
| **生产者/消费者拆成两个项目** | Producer 在 EC2-1，Consumer 在 EC2-3 | 独立部署、独立扩展、职责分离 |
| **symbol 作 Kafka key** | `send(TOPIC, kline.getSymbol(), kline)` | 同一交易对进同一 partition，保证 per-symbol 顺序 |
| **UPSERT 幂等** | `INSERT ON CONFLICT (symbol,open_time,close_time) DO NOTHING` | Backfill/Realtime 重复或 Kafka 重投都安全去重 |
| **手动提交 offset** | `enable-auto-commit=false` + `ack.acknowledge()` | 写库成功才提交，崩溃可重放，at-least-once + 幂等 ≈ exactly-once |
| **并行 fetch** | `IntStream.range().parallel()` | 绕过 Binance 单次 1000 条限制，~10 路并发提速 |
| **异步批量发送** | `CompletableFuture.allOf()` | 不等单条 ACK，44640 条发送从 1.7s → 0.3s |
| **Batch Listener** | `setBatchListener(true)`, max.poll.records=2000 | 一次 poll 多条，减少方法调用开销 |
| **参数白名单校验** | 启动缓存 ~2000 个 symbol，请求先查白名单 | 挡住脏请求，避免无效查询打到下游 |
| **AOP 日志** | `@Around` 记录聚合耗时 | 业务代码干净，可观测性 |
| **统一异常处理** | `@RestControllerAdvice` | 输入错误统一返回 400，不暴露内部细节 |

---

## 关键配置

### Producer application.properties (EC2-1)
```properties
spring.application.name=kline-producer
spring.datasource.url=jdbc:postgresql://<RDS_ENDPOINT>.us-east-2.rds.amazonaws.com:5432/klinedb
spring.datasource.username=postgres
spring.datasource.password=${DB_PASSWORD}          # 环境变量，不硬编码
spring.datasource.driver-class-name=org.postgresql.Driver
mybatis.type-aliases-package=com.example.demo.model
mybatis.configuration.map-underscore-to-camel-case=true
spring.kafka.bootstrap-servers=172.31.2.188:9092   # EC2-2 私网 IP
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer
url-template=https://api.binance.us/api/v3/klines?symbol=%s&interval=1m&startTime=%s&endTime=%s&limit=%s
```

### Consumer application.properties (EC2-3)
```properties
spring.application.name=kline-consumer
spring.datasource.url=jdbc:postgresql://<RDS_ENDPOINT>.us-east-2.rds.amazonaws.com:5432/klinedb
spring.datasource.username=postgres
spring.datasource.password=${DB_PASSWORD}
mybatis.type-aliases-package=com.example.consumer.model
mybatis.configuration.map-underscore-to-camel-case=true
spring.kafka.bootstrap-servers=172.31.2.188:9092
spring.kafka.consumer.group-id=kline-db-writer-group
spring.kafka.consumer.auto-offset-reset=earliest
spring.kafka.consumer.enable-auto-commit=false
spring.kafka.consumer.properties.spring.json.use.type.headers=false
spring.kafka.consumer.properties.spring.json.value.default.type=com.example.consumer.model.Kline
spring.kafka.listener.ack-mode=manual
```

### Kafka docker-compose.yml (EC2-2)
```yaml
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      JVMFLAGS: "-Xmx128m -Xms64m"     # t2.micro 内存优化
  kafka:
    image: confluentinc/cp-kafka:7.5.0
    ports: ["9092:9092"]
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://172.31.2.188:9092  # 私网 IP
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      KAFKA_LOG_RETENTION_HOURS: 168                  # 7 天
      KAFKA_HEAP_OPTS: "-Xmx256m -Xms128m"            # t2.micro 内存优化
    volumes: ["kafka-data:/var/lib/kafka/data"]
```

### 数据库表结构
```sql
CREATE TABLE kline (
    symbol      VARCHAR(20)        NOT NULL,
    open_time   BIGINT             NOT NULL,
    close_time  BIGINT             NOT NULL,
    open        DOUBLE PRECISION   NOT NULL,
    high        DOUBLE PRECISION   NOT NULL,
    low         DOUBLE PRECISION   NOT NULL,
    close       DOUBLE PRECISION   NOT NULL,
    volume      DOUBLE PRECISION   NOT NULL,
    CONSTRAINT kline_pkey PRIMARY KEY (symbol, open_time, close_time)
);
```

---

## 部署与启动命令

### SSH 连接
```bash
chmod 400 ~/Downloads/kline-key.pem
ssh -i ~/Downloads/kline-key.pem ec2-user@3.15.15.144     # Producer
ssh -i ~/Downloads/kline-key.pem ec2-user@18.216.59.173   # Kafka
ssh -i ~/Downloads/kline-key.pem ec2-user@18.227.21.108   # Consumer
```

### 启动 Kafka (EC2-2)
```bash
cd ~/kafka && docker-compose up -d
docker-compose ps                    # 确认 kafka + zookeeper Up
```

### 启动 Producer (EC2-1)
```bash
cd ~/KlineLoaderProject && mvn clean package -DskipTests
nohup java -Xmx256m -jar target/demo-0.0.1-SNAPSHOT.jar \
  --spring.datasource.password=<YOUR_DB_PASSWORD> > ~/producer.log 2>&1 &
```

### 启动 Consumer (EC2-3)
```bash
cd ~/KlineConsumerProject && mvn clean package -DskipTests
nohup java -Xmx256m -jar target/kline-consumer-0.0.1-SNAPSHOT.jar \
  --spring.datasource.password=<YOUR_DB_PASSWORD> > ~/consumer.log 2>&1 &
```

### 测试 Backfill
```bash
# 1 小时数据（61 条）
curl -X POST "http://3.15.15.144:8080/klineloader?symbol=BTCUSDT&startTime=1704067200000&endTime=1704070800000"

# 1 个月数据（44640 条）
curl -X POST "http://3.15.15.144:8080/klineloader?symbol=BTCUSDT&startTime=1704067200000&endTime=1706745540000&limit=1000"
```

### 验证数据
```bash
export PGPASSWORD="<YOUR_DB_PASSWORD>"
psql -h <RDS_ENDPOINT>.us-east-2.rds.amazonaws.com -U postgres -d klinedb \
  -c "SELECT symbol, COUNT(*) FROM kline GROUP BY symbol;"
```

---

## 性能优化记录

### 1 个月 Backfill (44,640 条) 性能基线

| 阶段 | 优化前 | 优化后 | 手段 |
|------|--------|--------|------|
| Binance API fetch | 1736ms（串行）| **315ms** | 串行 → 自定义 20 线程池（详见下方对比实验）|
| Kafka 发送 | ~1.7s | ~0.3s | 串行 for → CompletableFuture 异步并行 |
| Kafka→Consumer 传输 | <200ms | <200ms | 近实时 |
| Consumer 写库 | ~4.5s | ~2s | 单条 Listener → Batch Listener (max.poll=2000) |
| 最后 flush 等待 | 5s（scheduler）| 0s | Batch Listener 自然攒批，去掉 scheduler |
| **总端到端** | **~11s** | **~4.5s** | — |

### 优化细节
1. **Producer 异步批量发送**：`CompletableFuture.allOf(futures).join()` 一次性等所有发送完成
2. **Consumer Batch Listener**：`KafkaConfig.batchKafkaListenerContainerFactory`，`setBatchListener(true)`，一次 poll 拿 2000 条直接 `batchInsert`
3. **Consumer 调优参数**：`MAX_POLL_RECORDS=2000`, `FETCH_MIN_BYTES=100KB`, `FETCH_MAX_WAIT_MS=500`

---

## ⭐ 对比实验：Binance Fetch 三种并发模式（重点）

> 实测环境：EC2-1 t2.micro（**1 vCPU**），1 个月数据 = 44,640 条 = 45 次 Binance API 调用
> 接口：`POST /klineloader?...&mode=serial|parallel|pool`，只计时 fetch 阶段（不含 Kafka 发送）
> 每种模式跑 3 次取最佳

| 模式 | Run1 | Run2 | Run3 | **最佳** | 相对串行 |
|------|------|------|------|---------|---------|
| **serial**（串行 for） | 2330ms | 1867ms | 1736ms | **1736ms** | 1.0x |
| **parallel**（`IntStream.parallel()` ForkJoinPool） | 908ms | 825ms | 727ms | **727ms** | **2.4x** |
| **pool**（自定义 20 线程 ExecutorService） | 500ms | 327ms | 315ms | **315ms** | **5.5x** 🏆 |

### 核心洞察（面试金句）

> **并行流的并行度按 CPU 核数定，因为它为 CPU 密集型设计。但 fetch 是 I/O 密集型——90% 时间在等网络，瓶颈是网络不是 CPU。所以应该用自定义线程池开远多于核数的线程。**

```
为什么 parallel stream 只有 2.4x？
  IntStream.parallel() 用公共 ForkJoinPool
  并行度 = availableProcessors() - 1 = 1 - 1 ≈ 1
  → 在 1 vCPU 上几乎退化成串行，无法利用 I/O 等待

为什么 pool 能到 5.5x？
  20 个线程同时等 Binance 响应
  CPU 在线程"等待网络"时快速轮转调度
  1 个 CPU 也能轻松扛住 20 个 I/O-bound 线程
  → 多出来的 2.3x 完全来自"正确区分 I/O 密集 vs CPU 密集"
```

```
1 个 vCPU 的时间轴对比：

串行：  [等网络][CPU][等网络][CPU]...        CPU 大量空闲    1736ms
并行流：[等][等]  只有 ~1-2 线程              CPU 仍浪费      727ms
线程池：[等]×20   20 请求同时等，CPU 快速轮转  接近网络极限    315ms
```

### 结论与落地
- **生产默认改成 `mode=pool`**（`KlineLoadController` 默认值）
- 线程数 20 是经验值：再大收益递减（受 Binance rate limit 和单段网络延迟限制）
- 这是一个**真实、可复现、有数据**的优化故事，比单纯说"我用了并行"强得多

---

## 已修复的 Bug

| # | Bug | 修复 |
|---|-----|------|
| 1 | Consumer buffer 用 ArrayList（线程不安全） | 改 CopyOnWriteArrayList → 后续 Batch Listener 后不再需要 buffer |
| 2 | 最后一批 <BATCH_SIZE 永远不写库 | 先加 @Scheduled flush，后改 Batch Listener 自然解决 |
| 3 | @KafkaListener groupId 硬编码与 properties 不一致 | 改成 `${spring.kafka.consumer.group-id}` |
| 4 | enable-auto-commit=false 但无手动 ack → offset 不提交 | 加 `ack.acknowledge()` |
| 5 | Producer 没设 symbol key | `send(TOPIC, kline.getSymbol(), kline)` |
| 6 | LoadService 注入无用的 KlineMapper | 移除 |
| 7 | Producer/Consumer 同一个 App | 拆成两个独立项目 |
| 8 | Consumer 缺 jackson-databind | pom 改用 spring-boot-starter-web |
| 9 | Kafka Cluster ID 冲突（重启后） | docker-compose down -v 清卷重建 |
| 10 | 反序列化失败（Producer/Consumer 包名不同） | use.type.headers=false + value.default.type |

---

## 恢复上下文用的快照

> 如果触发上下文限制，`/compact` 或开新会话后，把下面这段贴给 AI 即可立即接上：

```
我在做一个分布式 K 线数据系统，3 台 EC2 + 1 RDS：
- EC2-1 Producer: 3.15.15.144 (kline-app-sg), 跑 KlineLoaderProject, Spring Boot :8080
- EC2-2 Kafka: 18.216.59.173 / 私网 172.31.2.188 (kline-kafka-sg), Docker 跑 Kafka+ZK
- EC2-3 Consumer: 18.227.21.108 (kline-consumer-sg), 跑 KlineConsumerProject
- RDS: <RDS_ENDPOINT>.us-east-2.rds.amazonaws.com:5432, db=klinedb, user=postgres, pwd=<YOUR_DB_PASSWORD>
- Key: ~/Downloads/kline-key.pem (三台共用)
- AI 通过 Read 读 /tmp/ec2-setup/kline-key.pem 后用 Bash ssh 直接操作（注意 key 文件需重建）
本地代码: ~/Downloads/Job Finding/kline/{KlineLoaderProject, KlineConsumerProject}
数据流: POST /klineloader → 并行fetch Binance → 异步发Kafka(symbol作key) → Consumer Batch Listener → batchInsert UPSERT → RDS
当前状态: 完整链路跑通，已做性能优化（异步发送+Batch Listener），1月数据44640条端到端~4.5s
```

---

## 待办 / 下一步

- [ ] **重启服务验证优化效果**：Consumer 优化代码已写但还没 build+重启
- [ ] **统一 Region**：EC2 在 us-east-1，RDS 在 us-east-2，跨区会有延迟和流量费
- [ ] **GitHub Actions CI/CD**：替代手动 build+scp+重启
- [ ] **数据质量校验**：扫描时间断档、重复、OHLCV 合理性
- [ ] **集成测试**：Testcontainers 启真实 Kafka+PostgreSQL
- [ ] **密码进 Secrets Manager**：当前明文，生产不可
- [ ] **每次用完 Stop 三台 EC2**（省钱）

---

## 面试可讲的亮点

1. **从单体到分布式的演进**：原本 Producer+Consumer 在一个 App，拆成独立服务，理解了职责分离与独立扩展
2. **Kafka 解耦的真实价值**：削峰（并行 fetch 爆发 vs DB 稳态写入）、故障隔离（Consumer 挂不影响 Producer）、幂等重放
3. **性能优化全链路定位**：从 11s → 4.5s，能说清每个阶段的瓶颈和手段
4. **踩过的坑**：Cluster ID 冲突、反序列化跨包、offset 不提交——都是真实生产会遇到的
5. **工程判断力**：知道什么时候该用 Kafka，什么时候是 over-engineering（1.7条/秒的 realtime 其实不需要 Kafka，Backfill 瞬时爆发才需要）
6. **最小权限安全**：SG 引用 SG，RDS 不公网暴露，密码用环境变量
```
