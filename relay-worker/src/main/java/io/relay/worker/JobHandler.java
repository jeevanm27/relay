package io.relay.worker;

/**
 * A job handler. <b>Handlers must be idempotent.</b> Relay gives at-least-once delivery: after a crash, a lease
 * expiry or a zombie takeover the same job can run again. Use {@link JobContext#executionId()} (job id + attempt)
 * and the job id to dedupe side effects.
 *
 * <p>A handler runs on a virtual thread outside of any database transaction. It should react to
 * {@link InterruptedException}: the worker interrupts it when the lease is lost or on forced shutdown.
 * Throw {@link NonRetryableException} to send the job straight to the dead-letter queue.
 */
@FunctionalInterface
public interface JobHandler {
    void handle(JobContext context) throws Exception;
}
