package com.example.consumer.repo;

import com.example.consumer.model.Kline;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

@Component
public class KlineCopyWriter {

    @Autowired
    private DataSource dataSource;

    /**
     * 用 PostgreSQL COPY 批量写入：
     * 1. COPY 到临时表（无约束，极快）
     * 2. INSERT ... SELECT ... ON CONFLICT DO NOTHING 转入主表（保留幂等）
     */
    public int copyInsert(List<Kline> klines) throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            try (Statement st = conn.createStatement()) {
                st.execute("CREATE TEMP TABLE IF NOT EXISTS kline_staging (LIKE kline)");
                st.execute("TRUNCATE kline_staging");
            }

            // 构造 CSV
            StringBuilder sb = new StringBuilder(klines.size() * 64);
            for (Kline k : klines) {
                sb.append(k.getSymbol()).append(',')
                  .append(k.getOpenTime()).append(',')
                  .append(k.getCloseTime()).append(',')
                  .append(k.getOpen()).append(',')
                  .append(k.getHigh()).append(',')
                  .append(k.getLow()).append(',')
                  .append(k.getClose()).append(',')
                  .append(k.getVolume()).append('\n');
            }

            CopyManager cm = conn.unwrap(PGConnection.class).getCopyAPI();
            cm.copyIn("COPY kline_staging FROM STDIN WITH (FORMAT csv)",
                      new StringReader(sb.toString()));

            int inserted;
            try (Statement st = conn.createStatement()) {
                inserted = st.executeUpdate(
                    "INSERT INTO kline SELECT * FROM kline_staging " +
                    "ON CONFLICT (symbol, open_time, close_time) DO NOTHING");
            }

            conn.commit();
            return inserted;
        }
    }
}
