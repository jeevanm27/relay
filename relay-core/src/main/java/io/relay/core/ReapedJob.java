package io.relay.core;

import java.util.UUID;

/** A job whose lease expired and was moved by the reaper to PENDING (retry) or DEAD (attempts exhausted). */
public record ReapedJob(UUID id, String queue, JobState newState) {
}
