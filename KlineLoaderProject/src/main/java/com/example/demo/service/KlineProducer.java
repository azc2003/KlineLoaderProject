package com.example.demo.service;

import com.example.demo.model.Kline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Service
public class KlineProducer {

    private static final Logger logger = LoggerFactory.getLogger(KlineProducer.class);

    // 双 Topic：backfill 与 realtime 物理隔离
    private static final String BACKFILL_TOPIC = "kline-backfill";
    private static final String REALTIME_TOPIC = "kline-realtime";

    @Autowired
    private KafkaTemplate<String, Kline> kafkaTemplate;

    /** Backfill：异步并行批量发送到 kline-backfill */
    public void sendAll(List<Kline> klines) {
        long start = System.currentTimeMillis();
        List<CompletableFuture<?>> futures = klines.stream()
            .map(k -> kafkaTemplate.send(BACKFILL_TOPIC, k.getSymbol(), k).toCompletableFuture())
            .collect(Collectors.toList());
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        long elapsed = System.currentTimeMillis() - start;
        logger.info("[BACKFILL] Sent {} klines in {}ms ({} msg/sec)",
            klines.size(), elapsed, elapsed > 0 ? klines.size() * 1000L / elapsed : 0);
    }

    /** Realtime：单条发送到 kline-realtime（每分钟每symbol一条，无需批量）*/
    public void sendRealtime(Kline kline) {
        kafkaTemplate.send(REALTIME_TOPIC, kline.getSymbol(), kline);
        logger.debug("[REALTIME] sent {} @ {}", kline.getSymbol(), kline.getOpenTime());
    }
}
