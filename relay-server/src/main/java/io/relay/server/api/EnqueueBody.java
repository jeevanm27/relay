package io.relay.server.api;

import java.time.Instant;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

/** Request body of POST /v1/jobs. Name fields are restricted so they stay safe as metric labels and log fields. */
public record EnqueueBody(
        @NotBlank @Pattern(regexp = "^[A-Za-z0-9][A-Za-z0-9._:-]{0,99}$") String tenant,
        @NotBlank @Pattern(regexp = "^[A-Za-z0-9][A-Za-z0-9._:-]{0,99}$") String queue,
        @NotBlank @Pattern(regexp = "^[A-Za-z0-9][A-Za-z0-9._:-]{0,99}$") String type,
        JsonNode payload,
        Integer priority,
        Instant runAt,
        @Min(1) @Max(1000) Integer maxAttempts,
        @Size(min = 1, max = 255) String idempotencyKey) {
}
