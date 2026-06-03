package com.example.demo.service;

import com.example.demo.model.Kline;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.client.RestTemplate;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Validated
@Service
public class BinanceAPIService {

    @Value("${url-template}")
    private String urlTemplate;

    @Autowired
    private RestTemplate restTemplate;

    /**
     * Calls Binance API to get exchangeInfo JSON.
     */
    public String getExchangeInfo() {
        String url = "https://api.binance.us/api/v3/exchangeInfo";
        return restTemplate.getForObject(url, String.class);
    }

    /**
     * Loads raw Kline data for a given symbol and time range.
     * Uses '1m' interval by default for all calls to Binance API.
     */
    public List<Kline> Load(@NotBlank String symbol,
                            @NotNull Long startTime,
                            @NotNull Long endTime,
                            Integer limit) {

        // Format API URL
        String url = String.format(urlTemplate, symbol, startTime, endTime, limit);

        ResponseEntity<String[][]> response = restTemplate.getForEntity(url, String[][].class);

        // Convert each row into a Kline object using parallel stream
        return Arrays.stream(response.getBody())
                .parallel()
                .map(row -> new Kline(
                        symbol,
                        Long.parseLong(row[0]),
                        Long.parseLong(row[6]),
                        Double.parseDouble(row[1]),
                        Double.parseDouble(row[2]),
                        Double.parseDouble(row[3]),
                        Double.parseDouble(row[4]),
                        Double.parseDouble(row[5])
                ))
                .collect(Collectors.toUnmodifiableList());
    }
}
