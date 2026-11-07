package io.relay.worker;

import java.time.Duration;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import io.relay.core.Job;
import io.relay.core.JobRepository;
import io.relay.core.JobState;

final class WorkerTestSupport {

    private WorkerTestSupport() {
    }

    static void await(String what, Duration timeout, BooleanSupplier condition) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
        throw new AssertionError("timed out after " + timeout + " waiting for: " + what);
    }

    static void awaitState(JobRepository repo, UUID id, JobState state, Duration timeout) {
        await("job " + id + " to reach " + state, timeout,
                () -> repo.findById(id).map(Job::state).orElse(null) == state);
    }

    static WorkerConfig fastConfig(String queue, String workerId) {
        return WorkerConfig.forQueue(queue).withWorkerId(workerId)
                .withIdle(Duration.ofMillis(5), Duration.ofMillis(20))
                .withShutdownGrace(Duration.ofSeconds(10));
    }
}
