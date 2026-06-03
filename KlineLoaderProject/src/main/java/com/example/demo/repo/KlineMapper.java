package com.example.demo.repo;

import com.example.demo.model.Kline;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import org.apache.ibatis.annotations.*;

import java.util.List;

/**
 * Mapper interface for accessing and storing Kline data using MyBatis.
 */
@Mapper
public interface KlineMapper {

    /**
     * Retrieves raw Kline data from the database based on symbol and time range.
     *
     * @param symbol    the trading pair (e.g., "BTCUSDT")
     * @param openTime  start timestamp in milliseconds
     * @param closeTime end timestamp in milliseconds
     * @return list of Kline records within the specified time range
     */
    @Select("""
        SELECT * FROM kline
        WHERE symbol = #{symbol}
          AND open_time >= #{openTime}
          AND close_time <= #{closeTime}
        ORDER BY open_time
    """)
    List<Kline> getRawKline(String symbol, Long openTime, Long closeTime);

    /**
     * Batch inserts Kline data into the database.
     * Duplicate entries (by symbol + open_time + close_time) are ignored.
     *
     * @param klineList the list of Kline records to insert
     * @return the number of rows inserted (may be less than list size due to conflict skips)
     */
    @Insert({
            "<script>",
            "INSERT INTO kline(symbol, open_time, close_time, open, high, low, close, volume)",
            "VALUES",
            "<foreach collection='list' item='item' separator=','>",
            "  (#{item.symbol}, #{item.openTime}, #{item.closeTime},",
            "   #{item.open}, #{item.high}, #{item.low}, #{item.close}, #{item.volume})",
            "</foreach>",
            "ON CONFLICT (symbol, open_time, close_time) DO NOTHING",
            "</script>"
    })
    int batchInsert(@Param("list") @NotEmpty List<@Valid Kline> klineList);
}
