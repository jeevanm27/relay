package io.relay.server;

import io.relay.core.JobRepository;
import io.relay.core.RetryPolicy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

@Configuration
public class RelayConfig {

    @Bean
    JobRepository jobRepository(NamedParameterJdbcTemplate jdbc, RelayProperties props) {
        return new JobRepository(jdbc, new RetryPolicy(props.retryBaseSecs(), props.retryCapSecs()));
    }
}
