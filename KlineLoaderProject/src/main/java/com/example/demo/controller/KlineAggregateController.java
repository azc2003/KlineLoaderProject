package com.example.demo.controller;

import com.example.demo.model.Kline;
import com.example.demo.service.KlineAggregateService;
import com.example.demo.service.ValidationService;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Controller for handling Kline aggregation requests.
 */
@Validated
@RestController
public class KlineAggregateController {

    @Autowired
    private KlineAggregateService klineAggregateService;

    @Autowired
    private ValidationService validationService;

    /**
     * Returns aggregated Kline data based on symbol, interval, and time range.
     *
     * @param symbol     the trading pair symbol (e.g., "BTCUSDT")
     * @param interval   the time interval (e.g., "1m", "1h", "1d")
     * @param startTime  start timestamp (milliseconds)
     * @param endTime    end timestamp (milliseconds)
     * @return aggregated Kline data list
     */
    @GetMapping("/klineAggregate")
    public ResponseEntity<List<Kline>> KlineLoad(
            @RequestParam @NotBlank String symbol,
            @RequestParam(defaultValue = "1m") String interval,
            @RequestParam @NotNull Long startTime,
            @RequestParam @NotNull Long endTime) {

        validationService.validationWithoutLimit(symbol, interval, startTime, endTime);
        List<Kline> aggregatedList = klineAggregateService.get(symbol, interval, startTime, endTime);

        return ResponseEntity.ok(aggregatedList);
    }
}
