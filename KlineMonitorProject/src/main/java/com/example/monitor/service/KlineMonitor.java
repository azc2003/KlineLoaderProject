package com.example.monitor.service;

import com.example.monitor.model.Kline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 模拟【风控团队】的下游 Consumer：
 * 独立 Consumer Group，订阅同一份 realtime 数据，
 * 实时检测价格异常波动，不写库、不影响其他 Consumer。
 */
@Service
public class KlineMonitor {

    private static final Logger logger = LoggerFactory.getLogger(KlineMonitor.class);

    // 价格变动告警阈值（默认 0.1%）
    @Value("${monitor.price-threshold:0.001}")
    private double threshold;

    // 记录每个 symbol 上一根收盘价
    private final Map<String, Double> prevClose = new ConcurrentHashMap<>();
    private long count = 0;

    @KafkaListener(topics = "kline-realtime", groupId = "kline-monitor-group")
    public void monitor(Kline k) {
        count++;
        Double prev = prevClose.get(k.getSymbol());
        prevClose.put(k.getSymbol(), k.getClose());

        if (prev != null && prev > 0) {
            double change = (k.getClose() - prev) / prev;
            double absPct = Math.abs(change) * 100;

            if (Math.abs(change) >= threshold) {
                String dir = change > 0 ? "📈 UP" : "📉 DOWN";
                logger.warn("⚠️  [RISK ALERT] {} {} {}% in 1min | {} -> {} | (total monitored: {})",
                    k.getSymbol(), dir, String.format("%.3f", absPct), prev, k.getClose(), count);
            } else {
                logger.info("   [monitor] {} ok ({}%) close={}",
                    k.getSymbol(), String.format("%.3f", absPct), k.getClose());
            }
        } else {
            logger.info("   [monitor] {} baseline close={}", k.getSymbol(), k.getClose());
        }
    }
}
