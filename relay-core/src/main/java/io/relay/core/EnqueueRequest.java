package io.relay.core;

import java.time.Instant;
import java.util.regex.Pattern;

/**
 * Validated request to enqueue a job. Validation lives here (not only in the REST layer) so that every caller of
 * the repository gets the same guarantees.
 */
public record EnqueueRequest(
        String tenantId,
        String queue,
        String type,
        String payload,
        int priority,
        Instant runAt,
        int maxAttempts,
        String idempotencyKey) {

    public static final int DEFAULT_MAX_ATTEMPTS = 5;
    public static final int MAX_PAYLOAD_BYTES = 256 * 1024;
    private static final Pattern NAME = Pattern.compile("^[A-Za-z0-9][A-Za-z0-9._:-]{0,99}$");

    public EnqueueRequest {
        requireName("tenantId", tenantId);
        requireName("queue", queue);
        requireName("type", type);
        if (payload == null || payload.isBlank()) {
            payload = "{}";
        }
        if (payload.length() > MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("payload too large (max " + MAX_PAYLOAD_BYTES + " characters)");
        }
        if (maxAttempts < 1 || maxAttempts > 1000) {
            throw new IllegalArgumentException("maxAttempts must be between 1 and 1000");
        }
        if (idempotencyKey != null && (idempotencyKey.isBlank() || idempotencyKey.length() > 255)) {
            throw new IllegalArgumentException("idempotencyKey must be 1..255 characters when present");
        }
    }

    public static EnqueueRequest of(String tenantId, String queue, String type, String payloadJson) {
        return new EnqueueRequest(tenantId, queue, type, payloadJson, 0, null, DEFAULT_MAX_ATTEMPTS, null);
    }

    public EnqueueRequest withPriority(int p) {
        return new EnqueueRequest(tenantId, queue, type, payload, p, runAt, maxAttempts, idempotencyKey);
    }

    public EnqueueRequest withRunAt(Instant at) {
        return new EnqueueRequest(tenantId, queue, type, payload, priority, at, maxAttempts, idempotencyKey);
    }

    public EnqueueRequest withMaxAttempts(int n) {
        return new EnqueueRequest(tenantId, queue, type, payload, priority, runAt, n, idempotencyKey);
    }

    public EnqueueRequest withIdempotencyKey(String key) {
        return new EnqueueRequest(tenantId, queue, type, payload, priority, runAt, maxAttempts, key);
    }

    private static void requireName(String field, String value) {
        if (value == null || !NAME.matcher(value).matches()) {
            throw new IllegalArgumentException(field + " must match " + NAME.pattern());
        }
    }
}
