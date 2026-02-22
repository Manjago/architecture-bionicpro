package bionicpro.reports.handler;

import bionicpro.reports.auth.JwtUtil;
import bionicpro.reports.clickhouse.ClickHouseClient;
import io.javalin.http.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Обработчик запросов на отчёты.
 * <p>
 * Два endpoint'а:
 * <ul>
 *   <li>{@code GET /reports/me} — userId из JWT (основной, для фронтенда через BFF)</li>
 *   <li>{@code GET /reports/{userId}} — userId из пути (проверяет совпадение с JWT sub)</li>
 * </ul>
 */
public class ReportHandler {

    private static final Logger log = LoggerFactory.getLogger(ReportHandler.class);

    private final ClickHouseClient ch;

    public ReportHandler(ClickHouseClient ch) {
        this.ch = ch;
    }

    /**
     * GET /reports/me
     * <p>
     * Фронтенд вызывает /api/reports/me → BFF проксирует как /reports/me.
     * userId извлекается из JWT claim "sub".
     */
    public void getMyReport(Context ctx) {
        int userId = extractAndValidateToken(ctx);
        if (userId < 0) return; // ответ уже отправлен

        fetchAndReturnReport(ctx, userId);
    }

    /**
     * GET /reports/{userId}
     * <p>
     * Прямой доступ по userId. Проверяет, что запрошенный userId совпадает
     * с sub из JWT — пользователь может запрашивать только свой отчёт.
     */
    public void getReport(Context ctx) {
        int tokenUserId = extractAndValidateToken(ctx);
        if (tokenUserId < 0) return; // ответ уже отправлен

        // Проверить авторизацию: отчёт только по себе
        String requestedParam = ctx.pathParam("userId");
        int requestedUserId;
        try {
            requestedUserId = Integer.parseInt(requestedParam);
        } catch (NumberFormatException e) {
            ctx.status(400).json(Map.of("error", "userId must be a number"));
            return;
        }

        if (tokenUserId != requestedUserId) {
            log.warn("Access denied: token sub={} requested userId={}",
                    tokenUserId, requestedUserId);
            ctx.status(403).json(Map.of(
                    "error", "Access denied. You can only view your own report."
            ));
            return;
        }

        fetchAndReturnReport(ctx, requestedUserId);
    }

    // ── Shared logic ───────────────────────────────────────────────────

    /**
     * Извлекает и валидирует JWT из Authorization header.
     *
     * @return userId (>= 0) при успехе, -1 при ошибке (ответ уже отправлен)
     */
    private int extractAndValidateToken(Context ctx) {
        String authHeader = ctx.header("Authorization");
        if (authHeader == null || !authHeader.startsWith("Bearer ")) {
            ctx.status(401).json(Map.of("error", "Missing or invalid Authorization header"));
            return -1;
        }

        String token = authHeader.substring("Bearer ".length());
        Map<String, Object> claims;
        try {
            claims = JwtUtil.parseClaims(token);
        } catch (Exception e) {
            log.warn("Failed to parse JWT: {}", e.getMessage());
            ctx.status(401).json(Map.of("error", "Invalid token"));
            return -1;
        }

        Object subClaim = claims.get("sub");
        if (subClaim == null) {
            ctx.status(401).json(Map.of("error", "Token missing 'sub' claim"));
            return -1;
        }

        try {
            return Integer.parseInt(subClaim.toString());
        } catch (NumberFormatException e) {
            ctx.status(400).json(Map.of("error", "JWT sub is not a valid user ID"));
            return -1;
        }
    }

    /**
     * Запрашивает отчёт из ClickHouse и возвращает JSON.
     */
    private void fetchAndReturnReport(Context ctx, int userId) {
        try {
            List<Map<String, Object>> rows = ch.queryUserReport(userId);

            if (rows.isEmpty()) {
                ctx.status(404).json(Map.of(
                        "error", "Report not found",
                        "message", "No report available for this user. "
                                + "Reports are generated daily by ETL process."
                ));
                return;
            }

            // Первая строка содержит имя/email (одинаковые для всех prosthesis_type)
            String customerName = rows.getFirst().getOrDefault("customer_name", "").toString();
            String customerEmail = rows.getFirst().getOrDefault("customer_email", "").toString();
            String reportUpdated = rows.getFirst().getOrDefault("report_updated", "").toString();

            ctx.json(Map.of(
                    "userId", userId,
                    "customerName", customerName,
                    "customerEmail", customerEmail,
                    "reportUpdated", reportUpdated,
                    "prostheses", rows.stream().map(row -> Map.of(
                            "prosthesisType", row.getOrDefault("prosthesis_type", ""),
                            "totalSignals", row.getOrDefault("total_signals", 0),
                            "avgAmplitude", row.getOrDefault("avg_amplitude", 0),
                            "avgFrequency", row.getOrDefault("avg_frequency", 0),
                            "avgDuration", row.getOrDefault("avg_duration", 0),
                            "minSignalTime", row.getOrDefault("min_signal_time", ""),
                            "maxSignalTime", row.getOrDefault("max_signal_time", "")
                    )).toList()
            ));

            log.info("Report served: userId={}, prostheses={}", userId, rows.size());

        } catch (Exception e) {
            log.error("ClickHouse query failed for userId={}: {}", userId, e.getMessage(), e);
            ctx.status(500).json(Map.of("error", "Internal server error"));
        }
    }
}
