package io.relay.server.api;

import java.net.URI;
import java.util.UUID;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.relay.core.EnqueueRequest;
import io.relay.core.EnqueueResult;
import io.relay.core.JobRepository;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/v1/jobs")
public class JobController {

    private final JobRepository repo;
    private final ObjectMapper mapper;
    private final MeterRegistry registry;

    public JobController(JobRepository repo, ObjectMapper mapper, MeterRegistry registry) {
        this.repo = repo;
        this.mapper = mapper;
        this.registry = registry;
    }

    /** 201 for a new job, 200 with the existing job when the (tenant, idempotencyKey) pair was already enqueued. */
    @PostMapping
    public ResponseEntity<JobView> enqueue(@Valid @RequestBody EnqueueBody body) throws JsonProcessingException {
        EnqueueRequest request = new EnqueueRequest(
                body.tenant(), body.queue(), body.type(),
                body.payload() == null || body.payload().isNull() ? "{}" : mapper.writeValueAsString(body.payload()),
                body.priority() == null ? 0 : body.priority(),
                body.runAt(),
                body.maxAttempts() == null ? EnqueueRequest.DEFAULT_MAX_ATTEMPTS : body.maxAttempts(),
                body.idempotencyKey());

        EnqueueResult result = repo.enqueue(request);
        JobView view = JobView.of(result.job(), mapper);
        if (!result.created()) {
            return ResponseEntity.ok(view);
        }
        Counter.builder("relay.jobs.enqueued")
                .description("Jobs accepted by the REST API")
                .tag("queue", request.queue())
                .register(registry)
                .increment();
        return ResponseEntity.created(URI.create("/v1/jobs/" + view.id())).body(view);
    }

    @GetMapping("/{id}")
    public JobView get(@PathVariable UUID id) {
        return repo.findById(id)
                .map(j -> JobView.of(j, mapper))
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "job not found: " + id));
    }
}
