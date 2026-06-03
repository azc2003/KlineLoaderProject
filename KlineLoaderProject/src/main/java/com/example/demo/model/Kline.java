package com.example.demo.model;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents a single candlestick (Kline) record.
 * Used for price aggregation of a trading symbol in a specific time interval.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Kline {

    /**
     * Trading pair symbol (e.g., "BTCUSDT").
     */
    @NotBlank
    private String symbol;

    /**
     * Opening timestamp in milliseconds.
     */
    @Min(0)
    @NotNull
    private Long openTime;

    /**
     * Closing timestamp in milliseconds.
     */
    @NotNull
    private Long closeTime;

    /**
     * Opening price.
     */
    @NotNull
    private Double open;

    /**
     * Highest price during the interval.
     */
    @NotNull
    private Double high;

    /**
     * Lowest price during the interval.
     */
    @NotNull
    private Double low;

    /**
     * Closing price.
     */
    @NotNull
    private Double close;

    /**
     * Total trading volume during the interval.
     */
    @NotNull
    private Double volume;
}
