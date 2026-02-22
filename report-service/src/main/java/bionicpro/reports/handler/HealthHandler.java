package bionicpro.reports.handler;

import bionicpro.reports.clickhouse.ClickHouseClient;
import io.javalin.http.Context;

import java.util.Map;

/**
 * GET /health
 * <p>
 * Проверяет доступность ClickHouse и возвращает статус.
 */
public class HealthHandler {

    private final ClickHouseClient ch;

    public HealthHandler(ClickHouseClient ch) {
        this.ch = ch;
    }

    public void check(Context ctx) {
        try {
            boolean chOk = ch.ping();
            if (chOk) {
                ctx.json(Map.of("status", "UP", "clickhouse", "connected"));
            } else {
                ctx.status(503).json(Map.of("status", "DOWN", "clickhouse", "unreachable"));
            }
        } catch (Exception e) {
            ctx.status(503).json(Map.of("status", "DOWN", "error", e.getMessage()));
        }
    }
}
