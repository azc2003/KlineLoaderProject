package com.example.demo.service;

import com.example.demo.DemoApplication;
import com.example.demo.model.Kline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.IntStream;

@Service
public class LoadService {

    private static final Logger logger = LoggerFactory.getLogger(DemoApplication.class);

    @Autowired
    private KlineProducer klineProducer;
    @Autowired
    private BinanceAPIService binanceAPIService;
    @Autowired
    private ValidationService validationService;

    /**
     * @param mode serial | parallel | pool
     */
    public void loadURL(String symbol, String outputInterval,
                        Long startTime, Long endTime, Integer limit, String mode) {

        validationService.validationWithLimit(symbol, outputInterval, startTime, endTime, limit);

        final long mins = 1;
        int finalLimit = (limit != null) ? Math.min(limit, 1000) : 1000;
        int hoursPerSegment = Math.max(1, finalLimit / (int) mins);
        long segmentMs = hoursPerSegment * mins * 60_000L;
        int segments = (int) Math.ceil((endTime - startTime + 1) / (double) segmentMs);

        long fetchStart = System.currentTimeMillis();
        List<Kline> allKlines = new CopyOnWriteArrayList<>();

        if ("serial".equals(mode)) {
            for (int i = 0; i < segments; i++) {
                long s = startTime + i * segmentMs;
                long e = Math.min(startTime + (i + 1) * segmentMs - 1, endTime);
                allKlines.addAll(binanceAPIService.Load(symbol, s, e, finalLimit));
            }
        } else if ("pool".equals(mode)) {
            // I/O 密集型：自定义 20 线程池
            ExecutorService pool = Executors.newFixedThreadPool(20);
            try {
                List<Future<?>> futures = new ArrayList<>();
                for (int i = 0; i < segments; i++) {
                    final int idx = i;
                    futures.add(pool.submit(() -> {
                        long s = startTime + idx * segmentMs;
                        long e = Math.min(startTime + (idx + 1) * segmentMs - 1, endTime);
                        allKlines.addAll(binanceAPIService.Load(symbol, s, e, finalLimit));
                    }));
                }
                for (Future<?> f : futures) f.get();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            } finally {
                pool.shutdown();
            }
        } else {
            // parallel stream（默认 ForkJoinPool，受 vCPU 限制）
            IntStream.range(0, segments).parallel().forEach(i -> {
                long s = startTime + i * segmentMs;
                long e = Math.min(startTime + (i + 1) * segmentMs - 1, endTime);
                allKlines.addAll(binanceAPIService.Load(symbol, s, e, finalLimit));
            });
        }

        allKlines.sort(Comparator.comparingLong(Kline::getOpenTime));
        long fetchElapsed = System.currentTimeMillis() - fetchStart;

        logger.info("FETCH_BENCHMARK | mode={} | segments={} | klines={} | fetchMs={}",
            mode, segments, allKlines.size(), fetchElapsed);

        klineProducer.sendAll(new ArrayList<>(allKlines));
    }
}
