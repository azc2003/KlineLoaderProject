package com.example.consumer.repo;

import com.example.consumer.model.Kline;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface KlineMapper {

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
    int batchInsert(@Param("list") List<Kline> klineList);
}
