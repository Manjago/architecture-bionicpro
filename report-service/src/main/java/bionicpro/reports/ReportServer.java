package bionicpro.reports;

import bionicpro.reports.clickhouse.ClickHouseClient;
import bionicpro.reports.handler.HealthHandler;
import bionicpro.reports.handler.ReportHandler;
import io.javalin.Javalin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BionicPRO Report Service.
 * <p>
 * API для получения пользовательских отчётов из ClickHouse.
 * Авторизация: JWT из Authorization header (проксируется через BFF).
 * Пользователь может запрашивать только свой отчёт.
 */
public class ReportServer {

    private static final Logger log = LoggerFactory.getLogger(ReportServer.class);

    public static void main(String[] args) {
        // ── Config from env ────────────────────────────────────────────
        int port = intEnv("PORT", 8001);
        String chHost = env("CLICKHOUSE_HOST", "olap_db");
        int chPort = intEnv("CLICKHOUSE_PORT", 8123);

        // ── ClickHouse client ──────────────────────────────────────────
        var ch = new ClickHouseClient(chHost, chPort);

        // ── Handlers ───────────────────────────────────────────────────
        var reportHandler = new ReportHandler(ch);
        var healthHandler = new HealthHandler(ch);

        // ── Server ─────────────────────────────────────────────────────
        var app = Javalin.create(config -> {
            config.showJavalinBanner = false;
        });

        app.get("/reports/{userId}", reportHandler::getReport);
        app.get("/health", healthHandler::check);

        app.start(port);
        log.info("Report Service started on port {}, ClickHouse={}:{}",
                port, chHost, chPort);
    }

    // ── Helpers ────────────────────────────────────────────────────────

    static String env(String key, String defaultValue) {
        String val = System.getenv(key);
        return val != null && !val.isBlank() ? val : defaultValue;
    }

    static int intEnv(String key, int defaultValue) {
        try {
            return Integer.parseInt(System.getenv(key));
        } catch (Exception e) {
            return defaultValue;
        }
    }
}
