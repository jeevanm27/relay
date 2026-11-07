package io.relay.worker;

import java.util.UUID;

import io.relay.core.Job;

/** What a handler receives for one execution of a job. */
public record JobContext(Job job) {

    public UUID jobId() {
        return job.id();
    }

    /** 1-based claim counter; with {@link #jobId()} it identifies this execution. */
    public int attempt() {
        return job.attempt();
    }

    /** "jobId:attempt". Unique per execution, stable across nothing else: use jobId to dedupe across executions. */
    public String executionId() {
        return job.executionId();
    }

    /** Raw JSON payload. */
    public String payload() {
        return job.payload();
    }
}
