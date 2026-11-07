package io.relay.worker;

import java.time.Duration;
import java.util.UUID;

/**
 * @param maxConcurrency  maximum handlers running at once (bounded by a Semaphore)
 * @param leaseSecs       lease granted by every claim and by every heartbeat; heartbeats run every leaseSecs/3
 * @param claimBatch      upper bound of jobs fetched per claim call (the real limit is min(this, free slots))
 * @param idleMin         lower bound of the jittered sleep when the queue is empty
 * @param idleMax         upper bound of the jittered sleep when the queue is empty
 * @param shutdownGrace   how long stop() waits for in-flight handlers before abandoning them to lease expiry
 */
public record WorkerConfig(
        String queue,
        String workerId,
        int maxConcurrency,
        double leaseSecs,
        int claimBatch,
        Duration idleMin,
        Duration idleMax,
        Duration shutdownGrace) {

    public WorkerConfig {
        if (queue == null || queue.isBlank()) {
            throw new IllegalArgumentException("queue is required");
        }
        if (workerId == null || workerId.isBlank()) {
            throw new IllegalArgumentException("workerId is required");
        }
        if (maxConcurrency < 1) {
            throw new IllegalArgumentException("maxConcurrency must be >= 1");
        }
        if (leaseSecs <= 0) {
            throw new IllegalArgumentException("leaseSecs must be positive");
        }
        if (claimBatch < 1) {
            throw new IllegalArgumentException("claimBatch must be >= 1");
        }
        if (idleMin.isNegative() || idleMax.compareTo(idleMin) < 0) {
            throw new IllegalArgumentException("need 0 <= idleMin <= idleMax");
        }
    }

    public static WorkerConfig forQueue(String queue) {
        return new WorkerConfig(queue, "worker-" + UUID.randomUUID().toString().substring(0, 8),
                10, 30.0, 50, Duration.ofMillis(50), Duration.ofMillis(250), Duration.ofSeconds(30));
    }

    public WorkerConfig withWorkerId(String v) {
        return new WorkerConfig(queue, v, maxConcurrency, leaseSecs, claimBatch, idleMin, idleMax, shutdownGrace);
    }

    public WorkerConfig withMaxConcurrency(int v) {
        return new WorkerConfig(queue, workerId, v, leaseSecs, claimBatch, idleMin, idleMax, shutdownGrace);
    }

    public WorkerConfig withLeaseSecs(double v) {
        return new WorkerConfig(queue, workerId, maxConcurrency, v, claimBatch, idleMin, idleMax, shutdownGrace);
    }

    public WorkerConfig withClaimBatch(int v) {
        return new WorkerConfig(queue, workerId, maxConcurrency, leaseSecs, v, idleMin, idleMax, shutdownGrace);
    }

    public WorkerConfig withIdle(Duration min, Duration max) {
        return new WorkerConfig(queue, workerId, maxConcurrency, leaseSecs, claimBatch, min, max, shutdownGrace);
    }

    public WorkerConfig withShutdownGrace(Duration v) {
        return new WorkerConfig(queue, workerId, maxConcurrency, leaseSecs, claimBatch, idleMin, idleMax, v);
    }

    /** Heartbeat period: a third of the lease, so two consecutive heartbeats can fail before the lease expires. */
    public long heartbeatMillis() {
        return Math.max(10, Math.round(leaseSecs * 1000 / 3));
    }
}
