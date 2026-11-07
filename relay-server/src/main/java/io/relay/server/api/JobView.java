package io.relay.server.api;

import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.relay.core.Job;

/** JSON representation of a job. */
public record JobView(
        UUID id,
        String tenant,
        String queue,
        String type,
        JsonNode payload,
        String state,
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

    public static JobView of(Job j, ObjectMapper mapper) {
        JsonNode payload;
        try {
            payload = mapper.readTree(j.payload());
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("stored payload is not valid JSON for job " + j.id(), e);
        }
        return new JobView(j.id(), j.tenantId(), j.queue(), j.type(), payload, j.state().name(), j.priority(),
                j.runAt(), j.attempt(), j.maxAttempts(), j.leaseOwner(), j.leaseExpiresAt(), j.idempotencyKey(),
                j.lastError(), j.createdAt(), j.updatedAt(), j.startedAt(), j.finishedAt());
    }
}
