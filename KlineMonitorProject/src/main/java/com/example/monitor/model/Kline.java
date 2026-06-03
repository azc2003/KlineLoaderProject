package com.example.monitor.model;
import lombok.*;
@Data @NoArgsConstructor @AllArgsConstructor
public class Kline {
    private String symbol;
    private Long openTime;
    private Long closeTime;
    private Double open;
    private Double high;
    private Double low;
    private Double close;
    private Double volume;
}
