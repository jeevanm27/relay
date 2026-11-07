package io.relay.core;

/**
 * Full-jitter exponential backoff parameters. The delay for a failed attempt n is
 * {@code random() * min(capSecs, baseSecs * 2^n)}, computed inside PostgreSQL.
 */
public record RetryPolicy(double baseSecs, double capSecs) {

    public static final RetryPolicy DEFAULT = new RetryPolicy(1.0, 300.0);

    public RetryPolicy {
        if (baseSecs <= 0 || capSecs <= 0) {
            throw new IllegalArgumentException("baseSecs and capSecs must be positive");
        }
    }

    /** Upper bound of the delay after the given (failed) attempt. Used by tests and docs; PostgreSQL does the real work. */
    public double maxDelaySecs(int attempt) {
        return Math.min(capSecs, baseSecs * Math.pow(2, Math.min(attempt, 60)));
    }
}
