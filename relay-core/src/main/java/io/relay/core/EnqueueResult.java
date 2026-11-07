package io.relay.core;

/** {@code created=false} means an idempotency-key duplicate: {@code job} is the pre-existing job. */
public record EnqueueResult(Job job, boolean created) {
}
