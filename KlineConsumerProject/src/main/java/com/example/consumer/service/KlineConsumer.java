package com.example.consumer.service;

import com.example.consumer.model.Kline;
import com.example.consumer.repo.KlineCopyWriter;
import com.example.consumer.repo.KlineMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class KlineConsumer {

    private static final Logger logger = LoggerFactory.getLogger(KlineConsumer.class);

    @Autowired
    private KlineMapper klineMapper;
    @Autowired
    private KlineCopyWriter klineCopyWriter;

    @Value("${kafka.write-mode:insert}")
    private String writeMode;

    /** Realtime Listener：独立线程 + 低延迟 Factory */
    @KafkaListener(topics = "kline-realtime", containerFactory = "realtimeFactory")
    public void consumeRealtime(List<Kline> klines, Acknowledgment ack) {
        write(klines, ack, "REALTIME");
    }

    /** Backfill Listener：独立线程 + 高吞吐 Factory */
    @KafkaListener(topics = "kline-backfill", containerFactory = "backfillFactory")
    public void consumeBackfill(List<Kline> klines, Acknowledgment ack) {
        write(klines, ack, "BACKFILL");
    }

    /** 共享写库逻辑：两条流都用同一套 COPY/INSERT + UPSERT 去重 */
    private void write(List<Kline> klines, Acknowledgment ack, String src) {
        if (klines == null || klines.isEmpty()) { ack.acknowledge(); return; }
        long start = System.currentTimeMillis();
        try {
            int inserted = "copy".equals(writeMode)
                ? klineCopyWriter.copyInsert(klines)
                : klineMapper.batchInsert(klines);
            long ms = System.currentTimeMillis() - start;
            logger.info("[{}] inserted {}/{} | mode={} | skipped {} | {}ms | {} rec/s",
                src, inserted, klines.size(), writeMode, klines.size() - inserted, ms,
                ms > 0 ? klines.size() * 1000L / ms : 0);
            ack.acknowledge();
        } catch (Exception e) {
            logger.error("[{}] insert failed for {} records: {}", src, klines.size(), e.getMessage());
        }
    }
}
