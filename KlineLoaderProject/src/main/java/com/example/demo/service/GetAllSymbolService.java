package com.example.demo.service;

import jakarta.annotation.PostConstruct;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class GetAllSymbolService {

    private final List<String> allSymbols = new ArrayList<>();

    @Autowired
    private BinanceAPIService binanceAPIService;

    /**
     * Called once after bean initialization.
     * Fetches and caches all valid symbols from Binance exchangeInfo.
     */
    @PostConstruct
    public void initialize() {
        try {
            String response = binanceAPIService.getExchangeInfo();
            JSONObject json = new JSONObject(response);
            JSONArray symbolsArray = json.getJSONArray("symbols");

            for (int i = 0; i < symbolsArray.length(); i++) {
                JSONObject symbolObj = symbolsArray.getJSONObject(i);
                allSymbols.add(symbolObj.getString("symbol"));
            }

        } catch (Exception e) {
            // Log and rethrow as runtime exception
            throw new RuntimeException("Failed to fetch Binance exchangeInfo", e);
        }
    }

    /**
     * Checks whether the given symbol is valid.
     */
    public boolean isValidSymbol(String symbol) {
        return allSymbols.contains(symbol);
    }
}
