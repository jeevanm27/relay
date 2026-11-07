package io.relay.worker;

import io.relay.core.FailResult;
import io.relay.core.Job;

/**
 * Observation hooks. Used by tests and by the chaos harness to audit what the database accepted. Callbacks run on the
 * handler's virtual thread after the database call returned; they must not throw (exceptions are logged and ignored).
 */
public interface WorkerListener {

    /** complete() returned {@code accepted}. {@code false} means this worker was stale (fencing rejection). */
    default void onComplete(Job job, boolean accepted) {
    }

    /** fail() returned {@code result}; {@code result.accepted() == false} means stale. */
    default void onFail(Job job, FailResult result) {
    }

    /** A heartbeat returned zero rows: the lease is lost and the handler was interrupted. */
    default void onLeaseLost(Job job) {
    }

    WorkerListener NONE = new WorkerListener() {
    };
}
