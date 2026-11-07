package io.relay.worker;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.relay.core.FailResult;
import io.relay.core.Job;
import io.relay.core.JobRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Polls {@link JobRepository#claim}, runs handlers on virtual threads bounded by a {@link Semaphore}, and reports
 * results with fenced complete()/fail().
 *
 * <p>Transactions: claim, heartbeat, complete and fail are each one short statement. A handler never runs inside a
 * transaction and never holds a connection on behalf of the worker.
 *
 * <p>Leases: a scheduler thread heartbeats every in-flight job every leaseSecs/3. If a heartbeat updates zero rows
 * the lease is lost: the handler thread is interrupted, complete()/fail() are never called for that attempt, and a
 * warning is logged.
 */
public final class Worker implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Worker.class);

    private final JobRepository repo;
    private final WorkerConfig config;
    private final Map<String, JobHandler> handlers;
    private final WorkerMetrics metrics;
    private final WorkerListener listener;

    private final Semaphore slots;
    private final ExecutorService handlerThreads = Executors.newVirtualThreadPerTaskExecutor();
    private final ScheduledExecutorService heartbeatThread = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "relay-heartbeat");
        t.setDaemon(true);
        return t;
    });
    private final Map<UUID, Execution> inFlight = new ConcurrentHashMap<>();
    private final CountDownLatch stopSignal = new CountDownLatch(1);
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean stopped = new AtomicBoolean();
    private volatile boolean accepting = true;
    private Thread poller;

    public Worker(JobRepository repo, WorkerConfig config, Map<String, JobHandler> handlers,
            MeterRegistry registry, WorkerListener listener) {
        this.repo = repo;
        this.config = config;
        this.handlers = Map.copyOf(handlers);
        this.metrics = new WorkerMetrics(registry, config.queue());
        this.listener = listener == null ? WorkerListener.NONE : listener;
        this.slots = new Semaphore(config.maxConcurrency());
    }

    public Worker(JobRepository repo, WorkerConfig config, Map<String, JobHandler> handlers) {
        this(repo, config, handlers, new SimpleMeterRegistry(), WorkerListener.NONE);
    }

    public WorkerConfig config() {
        return config;
    }

    public int inFlightCount() {
        return inFlight.size();
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("already started");
        }
        long hb = config.heartbeatMillis();
        heartbeatThread.scheduleAtFixedRate(this::heartbeatAll, hb, hb, TimeUnit.MILLISECONDS);
        poller = new Thread(this::pollLoop, "relay-poller-" + config.workerId());
        poller.start();
        log.info("worker {} started: queue={} concurrency={} lease={}s heartbeat={}ms batch={}",
                config.workerId(), config.queue(), config.maxConcurrency(), config.leaseSecs(), hb, config.claimBatch());
    }

    // ------------------------------------------------------------------ polling

    private void pollLoop() {
        while (accepting) {
            try {
                // wait for at least one free slot, then reserve every free slot we are allowed to fill
                if (!slots.tryAcquire(200, TimeUnit.MILLISECONDS)) {
                    continue;
                }
                int free = 1 + slots.drainPermits();
                int want = Math.min(free, config.claimBatch());
                slots.release(free - want);

                List<Job> jobs;
                try {
                    jobs = accepting ? repo.claim(config.queue(), config.workerId(), config.leaseSecs(), want) : List.of();
                } catch (RuntimeException e) {
                    slots.release(want);
                    log.warn("claim failed, backing off: {}", e.toString());
                    idleSleep();
                    continue;
                }
                slots.release(want - jobs.size());
                for (Job job : jobs) {
                    dispatch(job);
                }
                if (jobs.isEmpty()) {
                    idleSleep();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void idleSleep() throws InterruptedException {
        long min = config.idleMin().toMillis();
        long max = config.idleMax().toMillis();
        long ms = min == max ? min : ThreadLocalRandom.current().nextLong(min, max + 1);
        stopSignal.await(ms, TimeUnit.MILLISECONDS);
    }

    private void dispatch(Job job) {
        Execution ex = new Execution(job);
        inFlight.put(job.id(), ex);
        if (job.attempt() == 1 && job.startedAt() != null) {
            // both timestamps were written by PostgreSQL's now(): no JVM clock involved
            metrics.pickup(Duration.between(job.createdAt(), job.startedAt()));
        }
        try {
            handlerThreads.execute(() -> execute(ex));
        } catch (RuntimeException e) {
            // could not even start the thread: give up the slot, the lease will expire and the job will be retried
            inFlight.remove(job.id());
            slots.release();
            log.error("could not dispatch job {}", job.id(), e);
        }
    }

    // ------------------------------------------------------------------ execution

    private void execute(Execution ex) {
        Job job = ex.job;
        try {
            ex.thread = Thread.currentThread();
            long startNanos = System.nanoTime(); // monotonic, only for the duration metric; never used for leases
            Throwable failure = null;
            try {
                JobHandler handler = handlers.get(job.type());
                if (handler == null) {
                    throw new NonRetryableException("no handler registered for job type '" + job.type() + "'");
                }
                handler.handle(new JobContext(job));
            } catch (Throwable t) {
                failure = t;
            }
            ex.handlerDone = true;
            Thread.interrupted(); // clear a late interrupt so it cannot leak into the JDBC calls below
            metrics.duration(Duration.ofNanos(System.nanoTime() - startNanos));

            if (ex.leaseLost) {
                log.warn("job {} attempt {}: lease was lost, not reporting a result", job.id(), job.attempt());
                return;
            }
            if (ex.abandoned) {
                log.warn("job {} attempt {}: abandoned at shutdown, left to lease expiry", job.id(), job.attempt());
                return;
            }
            if (failure == null) {
                reportSuccess(job);
            } else {
                reportFailure(job, failure);
            }
        } catch (Throwable t) {
            log.error("unexpected error while finishing job {} attempt {}", job.id(), job.attempt(), t);
        } finally {
            inFlight.remove(job.id());
            slots.release();
        }
    }

    private void reportSuccess(Job job) {
        Boolean accepted = withRetries("complete", job, () -> repo.complete(job.id(), job.attempt()));
        if (accepted == null) {
            return; // database unreachable: leave it to lease expiry (at-least-once)
        }
        if (accepted) {
            metrics.finished("succeeded");
        } else {
            stale("complete", job);
        }
        notifyListener(() -> listener.onComplete(job, accepted));
    }

    private void reportFailure(Job job, Throwable failure) {
        boolean retryable = !(failure instanceof NonRetryableException);
        String message = failure.getClass().getName() + ": " + failure.getMessage();
        FailResult result = withRetries("fail", job, () -> repo.fail(job.id(), job.attempt(), message, retryable));
        if (result == null) {
            return;
        }
        switch (result.outcome()) {
            case RETRIED -> {
                metrics.finished("retried");
                log.info("job {} attempt {} failed, retry in {}s: {}", job.id(), job.attempt(),
                        String.format("%.2f", result.delaySecs()), message);
            }
            case DEAD -> {
                metrics.finished("dead");
                log.warn("job {} attempt {} failed permanently: {}", job.id(), job.attempt(), message);
            }
            case STALE -> stale("fail", job);
        }
        notifyListener(() -> listener.onFail(job, result));
    }

    private void stale(String op, Job job) {
        metrics.fencingRejected();
        log.warn("fencing rejection: {}({}, attempt={}) was stale, another worker owns this job now",
                op, job.id(), job.attempt());
    }

    /** A completion lost to a transient DB error would force a needless re-execution, so retry a few times. */
    private <T> T withRetries(String op, Job job, Supplier<T> call) {
        RuntimeException last = null;
        for (int i = 0; i < 3; i++) {
            try {
                return call.get();
            } catch (RuntimeException e) {
                last = e;
                try {
                    Thread.sleep(100L * (i + 1));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        log.error("{} for job {} attempt {} failed; leaving it to lease expiry: {}", op, job.id(), job.attempt(),
                String.valueOf(last));
        return null;
    }

    private void notifyListener(Runnable r) {
        try {
            r.run();
        } catch (RuntimeException e) {
            log.warn("worker listener threw", e);
        }
    }

    // ------------------------------------------------------------------ heartbeats

    private void heartbeatAll() {
        for (Execution ex : inFlight.values()) {
            if (ex.handlerDone || ex.leaseLost || ex.abandoned) {
                continue;
            }
            Job job = ex.job;
            try {
                if (!repo.heartbeat(job.id(), job.attempt(), config.leaseSecs())) {
                    ex.leaseLost = true;
                    metrics.leaseLost();
                    log.warn("lease lost for job {} attempt {}: interrupting handler", job.id(), job.attempt());
                    Thread t = ex.thread;
                    if (t != null && !ex.handlerDone) {
                        t.interrupt();
                    }
                    notifyListener(() -> listener.onLeaseLost(job));
                }
            } catch (RuntimeException e) {
                // a database blip must not kill the heartbeat thread; the next tick retries
                log.warn("heartbeat for job {} failed: {}", job.id(), e.toString());
            }
        }
    }

    // ------------------------------------------------------------------ shutdown

    /**
     * Graceful stop: stop claiming, wait up to the grace period for in-flight handlers (heartbeats keep running),
     * then abandon whatever is left. Abandoned jobs keep their lease until it expires and the reaper requeues them.
     */
    public void stop() {
        if (!stopped.compareAndSet(false, true)) {
            return;
        }
        log.info("worker {} stopping: no new claims, waiting up to {} for {} in-flight jobs",
                config.workerId(), config.shutdownGrace(), inFlight.size());
        accepting = false;
        stopSignal.countDown();
        try {
            if (poller != null) {
                poller.join();
            }
            long deadline = System.nanoTime() + config.shutdownGrace().toNanos();
            while (!inFlight.isEmpty() && System.nanoTime() < deadline) {
                Thread.sleep(25);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (!inFlight.isEmpty()) {
            log.warn("grace period over: abandoning {} in-flight jobs to lease expiry", inFlight.size());
            for (Execution ex : inFlight.values()) {
                ex.abandoned = true;
                Thread t = ex.thread;
                if (t != null) {
                    t.interrupt();
                }
            }
        }
        heartbeatThread.shutdownNow();
        handlerThreads.shutdown();
        try {
            if (!handlerThreads.awaitTermination(5, TimeUnit.SECONDS)) {
                handlerThreads.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log.info("worker {} stopped", config.workerId());
    }

    @Override
    public void close() {
        stop();
    }

    private static final class Execution {
        final Job job;
        volatile Thread thread;
        volatile boolean handlerDone;
        volatile boolean leaseLost;
        volatile boolean abandoned;

        Execution(Job job) {
            this.job = job;
        }
    }
}
