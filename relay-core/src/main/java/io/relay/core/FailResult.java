package io.relay.core;

/**
 * Outcome of {@link JobRepository#fail}. {@code STALE} means the fencing check rejected the caller (zero rows
 * updated): the lease was lost and somebody else owns the job now.
 *
 * @param delaySecs delay until the retry becomes claimable; 0 unless outcome is RETRIED
 */
public record FailResult(Outcome outcome, double delaySecs) {

    public enum Outcome { STALE, RETRIED, DEAD }

    public static final FailResult STALE = new FailResult(Outcome.STALE, 0);

    public boolean accepted() {
        return outcome != Outcome.STALE;
    }
}
