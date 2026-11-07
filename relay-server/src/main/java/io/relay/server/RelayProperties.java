package io.relay.server;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param retryBaseSecs   base of the full-jitter backoff (only meaningful for components that call fail())
 * @param retryCapSecs    cap of the backoff
 * @param reaperBatch     rows per reaper statement
 * @param maxReaperLoops  safety bound on consecutive batches in one reaper run
 */
@ConfigurationProperties(prefix = "relay")
public record RelayProperties(
        @DefaultValue("1.0") double retryBaseSecs,
        @DefaultValue("300.0") double retryCapSecs,
        @DefaultValue("500") int reaperBatch,
        @DefaultValue("20") int maxReaperLoops) {
}
