package io.relay.worker.app;

import io.relay.worker.JobContext;
import io.relay.worker.JobHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The example handler: logs the payload. Trivially idempotent (logging twice is harmless). */
public final class LogPayloadHandler implements JobHandler {

    private static final Logger log = LoggerFactory.getLogger(LogPayloadHandler.class);

    @Override
    public void handle(JobContext ctx) {
        log.info("job {} (attempt {}, tenant {}) payload={}", ctx.jobId(), ctx.attempt(),
                ctx.job().tenantId(), ctx.payload());
    }
}
