package com.example.demo.service;

import com.example.demo.model.IntervalUnit;
import com.example.demo.model.Kline;
import com.example.demo.model.ex.InputException;
import com.example.demo.repo.KlineMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Service
public class KlineAggregateService {

    private static final Logger logger = LoggerFactory.getLogger(KlineAggregateService.class);
    private static final long MINUTE = 60_000L;
    private static final long WAIT_TIMEOUT_MS = 15_000L;

    @Autowired
    private KlineMapper klineMapper;
    @Autowired
    private ValidationService validationService;
    @Autowired
    private LoadService loadService;

    public List<Kline> get(String symbol, String interval, Long startTime, Long endTime) {
        validationService.validationWithoutLimit(symbol, interval, startTime, endTime);

        // 期望的 1 分钟 open_time 序列（与查询过滤 close_time<=end 对齐）
        long alignedStart = ((startTime + MINUTE - 1) / MINUTE) * MINUTE;
        long tMax = ((endTime - 59999) / MINUTE) * MINUTE;
        if (tMax < alignedStart) {
            throw new InputException("Time range too small");
        }
        int expected = (int) ((tMax - alignedStart) / MINUTE) + 1;

        List<Kline> data = klineMapper.getRawKline(symbol, startTime, endTime);

        // 检测缺口 → 自动 backfill → 等待补齐
        List<long[]> gaps = findGaps(data, alignedStart, tMax);
        if (!gaps.isEmpty()) {
            logger.warn("[GAP] {} gap(s) for {}, triggering backfill", gaps.size(), symbol);
            for (long[] g : gaps) {
                // 补 [gapStart, gapEnd] 这段缺失的分钟
                loadService.loadURL(symbol, "1m", g[0], g[1] + 59999, 1000, "pool");
            }
            data = waitUntilComplete(symbol, startTime, endTime, expected);
        }

        if (data == null || data.isEmpty()) {
            throw new InputException("Input for aggregate must not be empty");
        }

        int bucketInterval = IntervalUnit.parseIntervalToMinutes(interval);
        final List<Kline> finalData = data;
        return IntStream.range(0, finalData.size())
                .filter(i -> i % bucketInterval == 0)
                .parallel()
                .mapToObj(i -> aggregate(finalData.subList(i, Math.min(i + bucketInterval, finalData.size()))))
                .collect(Collectors.toList());
    }

    /** 找缺口：连续缺失合并成 [startOpenTime, endOpenTime] 区间 */
    private List<long[]> findGaps(List<Kline> data, long alignedStart, long tMax) {
        Set<Long> present = new HashSet<>();
        for (Kline k : data) present.add(k.getOpenTime());
        List<long[]> gaps = new ArrayList<>();
        long gapStart = -1, gapPrev = -1;
        for (long t = alignedStart; t <= tMax; t += MINUTE) {
            if (!present.contains(t)) {
                if (gapStart < 0) gapStart = t;
                gapPrev = t;
            } else if (gapStart >= 0) {
                gaps.add(new long[]{gapStart, gapPrev});
                gapStart = -1;
            }
        }
        if (gapStart >= 0) gaps.add(new long[]{gapStart, gapPrev});
        return gaps;
    }

    /** 轮询直到补齐 expected 条或超时（backfill 经 Kafka 异步入库）*/
    private List<Kline> waitUntilComplete(String symbol, long startTime, long endTime, int expected) {
        long deadline = System.currentTimeMillis() + WAIT_TIMEOUT_MS;
        List<Kline> data = klineMapper.getRawKline(symbol, startTime, endTime);
        while (data.size() < expected && System.currentTimeMillis() < deadline) {
            try { Thread.sleep(300); } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
            data = klineMapper.getRawKline(symbol, startTime, endTime);
        }
        logger.info("[GAP] fill result {}/{} for {}", data.size(), expected, symbol);
        return data;
    }

    public Kline aggregate(List<Kline> data) {
        Kline first = data.get(0);
        Kline last = data.get(data.size() - 1);

        double high = first.getHigh();
        double low = first.getLow();
        double volume = 0d;

        for (Kline k : data) {
            high = Math.max(high, k.getHigh());
            low = Math.min(low, k.getLow());
            volume += k.getVolume();
        }

        return new Kline(
                first.getSymbol(),
                first.getOpenTime(),
                last.getCloseTime(),
                first.getOpen(),
                high,
                low,
                last.getClose(),
                volume
        );
    }
}
