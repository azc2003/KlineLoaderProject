package com.example.demo.model;

import lombok.Getter;

/**
 * Enum representing time interval units used in Kline data aggregation.
 * Provides utility methods to parse and convert intervals like "1m", "2h", "1d" to minutes.
 */
@Getter
public enum IntervalUnit {
    MINUTES("m", 1),
    HOURS("h", 60),
    DAYS("d", 1440);

    private final String symbol;
    private final int multiplier;

    IntervalUnit(String symbol, int multiplier) {
        this.symbol = symbol;
        this.multiplier = multiplier;
    }

    /**
     * Resolves an IntervalUnit enum from its symbol (e.g., "m", "h", "d").
     *
     * @param symbol time unit symbol
     * @return corresponding IntervalUnit
     * @throws IllegalArgumentException if the symbol is invalid
     */
    public static IntervalUnit fromSymbol(String symbol) {
        for (IntervalUnit unit : values()) {
            if (unit.symbol.equals(symbol)) {
                return unit;
            }
        }
        throw new IllegalArgumentException("Invalid interval unit: " + symbol);
    }

    /**
     * Parses an interval string like "1m", "2h", "1d" and converts it to total minutes.
     *
     * @param outputInterval interval string
     * @return equivalent number of minutes
     * @throws IllegalArgumentException if the interval format or unit is invalid
     */
    public static int parseIntervalToMinutes(String outputInterval) {
        String numberPart = outputInterval.replaceAll("[^0-9]", "");
        String unitPart = outputInterval.replaceAll("[0-9]", "");

        if (numberPart.isEmpty() || unitPart.isEmpty()) {
            throw new IllegalArgumentException("Invalid interval format: " + outputInterval);
        }

        int value = Integer.parseInt(numberPart);
        IntervalUnit unit = IntervalUnit.fromSymbol(unitPart);

        return value * unit.getMultiplier();
    }
}
