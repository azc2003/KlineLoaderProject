package com.example.demo.service;

import com.example.demo.model.Kline;
import jakarta.annotation.PostConstruct;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.*;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.net.URI;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service
public class BinanceRealtimeProducer extends TextWebSocketHandler {

    private static final Logger logger = LoggerFactory.getLogger(BinanceRealtimeProducer.class);

    @Autowired
    private KlineProducer klineProducer;

    @Value("${realtime.enabled:false}")
    private boolean enabled;

    @Value("${realtime.symbols:btcusdt}")
    private String symbolsCsv;

    @Value("${realtime.ws-base:wss://stream.binance.us:9443}")
    private String wsBase;

    private final StandardWebSocketClient client = new StandardWebSocketClient();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private volatile int backoffSec = 1;

    @PostConstruct
    public void start() {
        if (!enabled) {
            logger.info("[REALTIME] disabled (realtime.enabled=false)");
            return;
        }
        connect();
    }

    private String buildUrl() {
        // 组合流：/stream?streams=btcusdt@kline_1m/ethusdt@kline_1m
        String streams = Arrays.stream(symbolsCsv.split(","))
            .map(String::trim).filter(s -> !s.isEmpty())
            .map(s -> s.toLowerCase() + "@kline_1m")
            .collect(Collectors.joining("/"));
        return wsBase + "/stream?streams=" + streams;
    }

    private void connect() {
        String url = buildUrl();
        logger.info("[REALTIME] connecting: {}", url);
        client.execute(this, new WebSocketHttpHeaders(), URI.create(url))
            .whenComplete((session, ex) -> {
                if (ex != null) {
                    logger.error("[REALTIME] connect failed: {}", ex.getMessage());
                    scheduleReconnect();
                }
            });
    }

    private void scheduleReconnect() {
        int delay = backoffSec;
        logger.warn("[REALTIME] reconnect in {}s", delay);
        scheduler.schedule(this::connect, delay, TimeUnit.SECONDS);
        backoffSec = Math.min(backoffSec * 2, 30);  // 指数退避，上限 30s
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        logger.info("[REALTIME] connected ✅ symbols=[{}]", symbolsCsv);
        backoffSec = 1;  // 重置退避
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        try {
            JSONObject root = new JSONObject(message.getPayload());
            JSONObject data = root.has("data") ? root.getJSONObject("data") : root;
            JSONObject k = data.getJSONObject("k");

            // 只处理已收盘的 K 线（x=true）
            if (!k.getBoolean("x")) return;

            Kline kline = new Kline(
                k.getString("s"),
                k.getLong("t"),
                k.getLong("T"),
                Double.parseDouble(k.getString("o")),
                Double.parseDouble(k.getString("h")),
                Double.parseDouble(k.getString("l")),
                Double.parseDouble(k.getString("c")),
                Double.parseDouble(k.getString("v"))
            );
            klineProducer.sendRealtime(kline);
            logger.info("[REALTIME] closed kline {} t={} c={}",
                kline.getSymbol(), kline.getOpenTime(), kline.getClose());
        } catch (Exception e) {
            logger.error("[REALTIME] parse error: {}", e.getMessage());
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        logger.warn("[REALTIME] disconnected: {} {}", status.getCode(), status.getReason());
        scheduleReconnect();
    }
}
