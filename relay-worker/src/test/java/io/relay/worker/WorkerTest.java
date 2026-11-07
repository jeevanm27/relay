package io.relay.worker;

import static io.relay.worker.WorkerTestSupport.await;
import static io.relay.worker.WorkerTestSupport.awaitState;
import static io.relay.worker.WorkerTestSupport.fastConfig;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.relay.core.EnqueueRequest;
import io.relay.core.FailResult;
import io.relay.core.Job;
import io.relay.core.JobRepository;
import io.relay.core.JobState;
import io.relay.core.RetryPolicy;
import io.relay.testing.PostgresSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

class WorkerTest {

    private NamedParameterJdbcTemplate jdbc;
    private JobRepository repo;

    @BeforeEach
    void setUp() {
        PostgresSupport.reset();
        jdbc = PostgresSupport.jdbc();
        repo = new JobRepository(jdbc, new RetryPolicy(0.01, 0.05));
    }

    private UUID enqueue(String type) {
        return repo.enqueue(EnqueueRequest.of("acme", "q", type, "{\"hello\":\"world\"}")).job().id();
    }

    @Test
    void runsJobsEndToEndAndNeverExceedsMaxConcurrency() {
        int jobs = 200;
        for (int i = 0; i < jobs; i++) {
            enqueue("work");
        }
        AtomicInteger running = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();
        ConcurrentHashMap<UUID, AtomicInteger> runs = new ConcurrentHashMap<>();
        JobHandler handler = ctx -> {
            peak.accumulateAndGet(running.incrementAndGet(), Math::max);
            runs.computeIfAbsent(ctx.jobId(), k -> new AtomicInteger()).incrementAndGet();
            Thread.sleep(10);
            running.decrementAndGet();
        };
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        try (Worker w = new Worker(repo, fastConfig("q", "w1").withMaxConcurrency(8).withClaimBatch(20),
                Map.of("work", handler), registry, WorkerListener.NONE)) {
            w.start();
            await("all jobs succeeded", Duration.ofSeconds(60), () -> count(JobState.SUCCEEDED) == jobs);
        }
        assertThat(peak.get()).isLessThanOrEqualTo(8).isGreaterThan(1);
        assertThat(runs).hasSize(jobs);
        assertThat(runs.values()).allSatisfy(n -> assertThat(n.get()).isEqualTo(1));
        assertThat(registry.get("relay.jobs.finished").tag("outcome", "succeeded").counter().count()).isEqualTo(jobs);
        assertThat(registry.get("relay.job.pickup").timer().count()).isEqualTo(jobs);
        assertThat(registry.get("relay.job.duration").timer().count()).isEqualTo(jobs);
    }

    @Test
    void alwaysFailingHandlerEndsDeadAfterExactlyMaxAttempts() {
        UUID id = repo.enqueue(EnqueueRequest.of("acme", "q", "boom", null).withMaxAttempts(4)).job().id();
        AtomicInteger calls = new AtomicInteger();
        List<FailResult> results = new CopyOnWriteArrayList<>();
        WorkerListener listener = new WorkerListener() {
            @Override
            public void onFail(Job job, FailResult result) {
                results.add(result);
            }
        };
        try (Worker w = new Worker(repo, fastConfig("q", "w1"), Map.of("boom", ctx -> {
            calls.incrementAndGet();
            throw new IllegalStateException("always");
        }), new SimpleMeterRegistry(), listener)) {
            w.start();
            awaitState(repo, id, JobState.DEAD, Duration.ofSeconds(30));
        }
        assertThat(calls.get()).isEqualTo(4);
        Job dead = repo.findById(id).orElseThrow();
        assertThat(dead.attempt()).isEqualTo(4);
        assertThat(dead.lastError()).contains("IllegalStateException").contains("always");
        assertThat(results).extracting(FailResult::outcome).containsExactly(
                FailResult.Outcome.RETRIED, FailResult.Outcome.RETRIED, FailResult.Outcome.RETRIED,
                FailResult.Outcome.DEAD);
    }

    @Test
    void nonRetryableExceptionGoesStraightToDeadAndUnknownTypeToo() {
        UUID bad = repo.enqueue(EnqueueRequest.of("acme", "q", "bad", null).withMaxAttempts(10)).job().id();
        UUID unknown = repo.enqueue(EnqueueRequest.of("acme", "q", "no-such-handler", null).withMaxAttempts(10)).job().id();
        try (Worker w = new Worker(repo, fastConfig("q", "w1"), Map.of("bad", ctx -> {
            throw new NonRetryableException("payload is garbage");
        }))) {
            w.start();
            awaitState(repo, bad, JobState.DEAD, Duration.ofSeconds(20));
            awaitState(repo, unknown, JobState.DEAD, Duration.ofSeconds(20));
        }
        assertThat(repo.findById(bad).orElseThrow().attempt()).isEqualTo(1);
        assertThat(repo.findById(unknown).orElseThrow().attempt()).isEqualTo(1);
    }

    @Test
    void heartbeatsKeepALongJobAliveWhileAReaperIsRunning() throws Exception {
        UUID id = enqueue("long");
        // lease 1.2s, handler 3.5s: only heartbeats (every 0.4s) keep it from being reaped
        Thread reaper = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                repo.reapExpired(100);
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    return;
                }
            }
        });
        reaper.start();
        AtomicInteger runs = new AtomicInteger();
        try (Worker w = new Worker(repo, fastConfig("q", "w1").withLeaseSecs(1.2), Map.of("long", ctx -> {
            runs.incrementAndGet();
            Thread.sleep(3_500);
        }))) {
            w.start();
            awaitState(repo, id, JobState.SUCCEEDED, Duration.ofSeconds(30));
        } finally {
            reaper.interrupt();
            reaper.join();
        }
        assertThat(runs.get()).isEqualTo(1);
        assertThat(repo.findById(id).orElseThrow().attempt()).isEqualTo(1);
    }

    @Test
    void lostLeaseInterruptsTheHandlerAndNeverReportsAResult() throws Exception {
        UUID id = enqueue("victim");
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        List<String> events = new CopyOnWriteArrayList<>();
        WorkerListener listener = new WorkerListener() {
            @Override
            public void onComplete(Job job, boolean accepted) {
                events.add("complete:" + accepted);
            }

            @Override
            public void onFail(Job job, FailResult result) {
                events.add("fail:" + result.outcome());
            }

            @Override
            public void onLeaseLost(Job job) {
                events.add("leaseLost");
            }
        };
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        try (Worker w = new Worker(repo, fastConfig("q", "w1").withLeaseSecs(0.9), Map.of("victim", ctx -> {
            started.countDown();
            try {
                Thread.sleep(30_000);
            } catch (InterruptedException e) {
                interrupted.countDown();
                throw e;
            }
        }), registry, listener)) {
            w.start();
            assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();

            // another actor takes the job: the lease is forced to expire, the reaper requeues it, a thief claims attempt 2
            jdbc.getJdbcTemplate().update("UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = ?", id);
            assertThat(repo.reapExpired(10)).hasSize(1);
            Job thief = repo.claim("q", "thief", 60, 1).get(0);
            assertThat(thief.attempt()).isEqualTo(2);

            // the next heartbeat (attempt 1) updates zero rows -> handler interrupted
            assertThat(interrupted.await(10, TimeUnit.SECONDS)).isTrue();
            await("lease lost event", Duration.ofSeconds(5), () -> events.contains("leaseLost"));
            Thread.sleep(300);
        }
        // the worker never called complete() or fail() for the lost attempt
        assertThat(events).containsExactly("leaseLost");
        assertThat(registry.get("relay.worker.leases.lost").counter().count()).isEqualTo(1);
        Job stillThiefs = repo.findById(id).orElseThrow();
        assertThat(stillThiefs.state()).isEqualTo(JobState.RUNNING);
        assertThat(stillThiefs.leaseOwner()).isEqualTo("thief");
        assertThat(stillThiefs.attempt()).isEqualTo(2);
    }

    @Test
    void zombieCompletionIsCountedAsAFencingRejection() throws Exception {
        UUID id = enqueue("slow");
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        List<String> events = new CopyOnWriteArrayList<>();
        WorkerListener listener = new WorkerListener() {
            @Override
            public void onComplete(Job job, boolean accepted) {
                events.add("complete:" + accepted);
            }
        };
        // lease 600s, so the heartbeat thread will not interfere: this isolates the fenced complete() path
        try (Worker w = new Worker(repo, fastConfig("q", "w1").withLeaseSecs(600), Map.of("slow", ctx -> {
            started.countDown();
            release.await();
        }), registry, listener)) {
            w.start();
            assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
            jdbc.getJdbcTemplate().update("UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = ?", id);
            repo.reapExpired(10);
            Job thief = repo.claim("q", "thief", 600, 1).get(0);
            assertThat(thief.attempt()).isEqualTo(2);

            release.countDown();
            await("fencing rejection", Duration.ofSeconds(10), () -> events.contains("complete:false"));
        }
        assertThat(registry.get("relay.fencing.rejections").counter().count()).isEqualTo(1);
        assertThat(repo.findById(id).orElseThrow().state()).isEqualTo(JobState.RUNNING);
    }

    @Test
    void gracefulShutdownFinishesInFlightJobsAndClaimsNothingNew() throws Exception {
        int total = 40;
        List<UUID> ids = new ArrayList<>();
        for (int i = 0; i < total; i++) {
            ids.add(enqueue("slowish"));
        }
        AtomicInteger started = new AtomicInteger();
        AtomicInteger finished = new AtomicInteger();
        Worker w = new Worker(repo, fastConfig("q", "w1").withMaxConcurrency(5).withClaimBatch(5),
                Map.of("slowish", ctx -> {
                    started.incrementAndGet();
                    Thread.sleep(800);
                    finished.incrementAndGet();
                }));
        w.start();
        await("5 jobs in flight", Duration.ofSeconds(10), () -> w.inFlightCount() == 5);

        w.stop(); // equivalent of SIGTERM handling: blocks until in-flight jobs finish or the grace period ends

        assertThat(w.inFlightCount()).isZero();
        assertThat(finished.get()).as("every started job finished").isEqualTo(started.get());
        long claimedAtStop = count("attempt >= 1");
        long succeeded = count(JobState.SUCCEEDED);
        assertThat(succeeded).isEqualTo(started.get());
        assertThat(count(JobState.RUNNING)).isZero();
        assertThat(claimedAtStop).as("nothing was claimed that did not run").isEqualTo(started.get());

        // no new claims after the signal, even after waiting well beyond the idle/poll interval
        Thread.sleep(1_000);
        assertThat(count("attempt >= 1")).isEqualTo(claimedAtStop);
        assertThat(count(JobState.PENDING)).isEqualTo(total - claimedAtStop);
        assertThat(ids).hasSize(total);
    }

    @Test
    void jobsStillRunningWhenTheGracePeriodEndsAreLeftToLeaseExpiry() throws Exception {
        UUID id = enqueue("stuck");
        CountDownLatch started = new CountDownLatch(1);
        Worker w = new Worker(repo, fastConfig("q", "w1").withLeaseSecs(1.5).withShutdownGrace(Duration.ofMillis(300)),
                Map.of("stuck", ctx -> {
                    started.countDown();
                    Thread.sleep(60_000);
                }));
        w.start();
        assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
        w.stop();

        // abandoned: no complete/fail was sent, and heartbeats stopped, so the lease simply runs out
        assertThat(repo.findById(id).orElseThrow().state()).isEqualTo(JobState.RUNNING);
        Thread.sleep(1_800);
        assertThat(repo.reapExpired(10)).hasSize(1);
        Job requeued = repo.findById(id).orElseThrow();
        assertThat(requeued.state()).isEqualTo(JobState.PENDING);
        assertThat(requeued.attempt()).isEqualTo(1);
    }

    private long count(JobState state) {
        return count("state = '" + state.name() + "'");
    }

    private long count(String where) {
        Long n = jdbc.getJdbcTemplate().queryForObject("SELECT count(*) FROM jobs WHERE " + where, Long.class);
        return n == null ? 0 : n;
    }
}
