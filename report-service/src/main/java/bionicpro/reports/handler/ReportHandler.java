package bionicpro.reports.handler;

import bionicpro.reports.auth.JwtUtil;
import bionicpro.reports.clickhouse.ClickHouseClient;
import io.javalin.http.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * GET /reports/{userId}
 * <p>
 * Возвращает агрегированный отчёт по протезам пользователя.
 * Авторизация: Bearer JWT → извлекаем sub (user_id) → сравниваем с {userId} в пути.
 * Пользователь может запрашивать только свой отчёт.
 */
public class ReportHandler {

    private static final Logger log = LoggerFactory.getLogger(ReportHandler.class);

    private final ClickHouseClient ch;

    public ReportHandler(ClickHouseClient ch) {
        this.ch = ch;
    }

    public void getReport(Context ctx) {
        // ── 1. Извлечь и проверить JWT ─────────────────────────────────
        String authHeader = ctx.header("Authorization");
        if (authHeader == null || !authHeader.startsWith("Bearer ")) {
            ctx.status(401).json(Map.of("error", "Missing or invalid Authorization header"));
            return;
        }

        String token = authHeader.substring("Bearer ".length());
        Map<String, Object> claims;
        try {
            claims = JwtUtil.parseClaims(token);
        } catch (Exception e) {
            log.warn("Failed to parse JWT: {}", e.getMessage());
            ctx.status(401).json(Map.of("error", "Invalid token"));
            return;
        }

        // ── 2. Извлечь user_id из JWT (claim "sub") ───────────────────
        Object subClaim = claims.get("sub");
        if (subClaim == null) {
            ctx.status(401).json(Map.of("error", "Token missing 'sub' claim"));
            return;
        }
        String tokenUserId = subClaim.toString();

        // ── 3. Проверить авторизацию: отчёт только по себе ─────────────
        String requestedUserId = ctx.pathParam("userId");

        if (!tokenUserId.equals(requestedUserId)) {
            log.warn("Access denied: token sub={} requested userId={}",
                    tokenUserId, requestedUserId);
            ctx.status(403).json(Map.of(
                    "error", "Access denied. You can only view your own report."
            ));
            return;
        }

        // ── 4. Запросить отчёт из ClickHouse ──────────────────────────
        int userId;
        try {
            userId = Integer.parseInt(requestedUserId);
        } catch (NumberFormatException e) {
            ctx.status(400).json(Map.of("error", "userId must be a number"));
            return;
        }

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

            // ── 5. Сформировать ответ ──────────────────────────────────
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
