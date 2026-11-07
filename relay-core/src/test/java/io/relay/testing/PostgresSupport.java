package io.relay.testing;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * One real PostgreSQL 16 container per test JVM (never H2, never a mocked database). Flyway migrates it once;
 * each test class calls {@link #reset()} to start from empty tables.
 */
public final class PostgresSupport {

    private static final PostgreSQLContainer<?> PG =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                    .withDatabaseName("relay")
                    .withUsername("relay")
                    .withPassword("relay")
                    .withCommand("postgres", "-c", "max_connections=300", "-c", "fsync=off");

    private static HikariDataSource shared;

    static {
        PG.start();
        Flyway.configure().dataSource(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword())
                .locations("classpath:db/migration").load().migrate();
    }

    private PostgresSupport() {
    }

    public static String jdbcUrl() {
        return PG.getJdbcUrl();
    }

    public static String username() {
        return PG.getUsername();
    }

    public static String password() {
        return PG.getPassword();
    }

    /** A new pool; the caller closes it. */
    public static HikariDataSource newDataSource(int poolSize) {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(PG.getJdbcUrl());
        cfg.setUsername(PG.getUsername());
        cfg.setPassword(PG.getPassword());
        cfg.setMaximumPoolSize(poolSize);
        cfg.setMinimumIdle(Math.min(poolSize, 2));
        cfg.setAutoCommit(true);
        return new HikariDataSource(cfg);
    }

    /** Shared small pool for tests that do not care about pool sizing. */
    public static synchronized DataSource dataSource() {
        if (shared == null) {
            shared = newDataSource(20);
        }
        return shared;
    }

    public static NamedParameterJdbcTemplate jdbc() {
        return new NamedParameterJdbcTemplate(dataSource());
    }

    /** Empties all queue tables. Called before each test. */
    public static void reset() {
        jdbc().getJdbcTemplate().execute("TRUNCATE TABLE jobs, tenants CASCADE");
    }
}
