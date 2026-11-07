package io.relay.core;

import java.time.Instant;
import java.util.UUID;

/**
 * A row of the {@code jobs} table. All timestamps come from PostgreSQL's {@code now()}, never from the JVM clock.
 * {@code attempt} is the fencing token: it identifies one claim of this job.
 */
public record Job(
        UUID id,
        String tenantId,
        String queue,
        String type,
        String payload,
        JobState state,
        int priority,
        Instant runAt,
        int attempt,
        int maxAttempts,
        String leaseOwner,
        Instant leaseExpiresAt,
        String idempotencyKey,
        String lastError,
        Instant createdAt,
        Instant updatedAt,
        Instant startedAt,
        Instant finishedAt) {

    /**
     * Identifies one execution of this job: handlers can dedupe on it. Two executions with the same
     * (job id, attempt) never happen because attempt is incremented by every claim.
     */
    public String executionId() {
        return id + ":" + attempt;
    }
}
