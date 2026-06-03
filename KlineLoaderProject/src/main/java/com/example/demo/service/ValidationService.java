package com.example.demo.service;

import com.example.demo.model.ex.InputException;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;

@Validated
@Service
public class ValidationService {

    @Autowired
    private GetAllSymbolService getAllSymbolService;

    /**
     * Validates inputs including optional limit.
     */
    public boolean validationWithLimit(@NotBlank String symbol,
                                       @NotBlank String interval,
                                       @NotNull long startTime,
                                       @NotNull long endTime,
                                       Integer limit) {

        validateCommon(symbol, interval, startTime, endTime);

        if (limit != null && (limit <= 0 || limit > 1000)) {
            throw new InputException("Limit must be between 1 and 1000.");
        }

        return true;
    }

    /**
     * Validates inputs without limit.
     */
    public boolean validationWithoutLimit(@NotBlank String symbol,
                                          @NotBlank String interval,
                                          @NotNull long startTime,
                                          @NotNull long endTime) {
        return validateCommon(symbol, interval, startTime, endTime);
    }

    private boolean validateCommon(String symbol, String interval, long startTime, long endTime) {
        if (startTime > endTime) {
            throw new InputException("startTime is " + startTime + ", endTime is " + endTime + ". startTime must be smaller than endTime.");
        }

        if (!getAllSymbolService.isValidSymbol(symbol)) {
            throw new InputException("Invalid symbol: " + symbol);
        }

        if (!interval.matches("^\\d+[mhd]$")) {
            throw new InputException("Invalid interval format: " + interval + ". Must be like '1m', '2h', '1d'");
        }

        int intervalValue = Integer.parseInt(interval.replaceAll("[^0-9]", ""));
        if (intervalValue < 1) {
            throw new InputException("Interval value must be at least 1.");
        }

        return true;
    }
}
