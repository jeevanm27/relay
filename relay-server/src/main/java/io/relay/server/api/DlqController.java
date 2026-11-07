package io.relay.server.api;

import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.relay.core.JobRepository;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/v1/dlq")
public class DlqController {

    public record ReplayBody(@NotNull @Min(1) @Max(1000) Integer extraAttempts) {
    }

    public record DlqPage(List<JobView> items, int page, int size, long total) {
    }

    private final JobRepository repo;
    private final ObjectMapper mapper;

    public DlqController(JobRepository repo, ObjectMapper mapper) {
        this.repo = repo;
        this.mapper = mapper;
    }

    @GetMapping
    public DlqPage list(
            @RequestParam(required = false) String tenant,
            @RequestParam(required = false) String queue,
            @RequestParam(defaultValue = "0") @Min(0) int page,
            @RequestParam(defaultValue = "50") @Min(1) @Max(200) int size) {
        List<JobView> items = repo.listDead(tenant, queue, size, page * size).stream()
                .map(j -> JobView.of(j, mapper)).toList();
        return new DlqPage(items, page, size, repo.countDead(tenant, queue));
    }

    /** Gives a DEAD job extra attempts. The attempt counter is never reset (fencing tokens must not be reused). */
    @PostMapping("/{id}/replay")
    public JobView replay(@PathVariable UUID id, @Valid @RequestBody ReplayBody body) {
        if (!repo.replay(id, body.extraAttempts())) {
            var job = repo.findById(id)
                    .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "job not found: " + id));
            throw new ResponseStatusException(HttpStatus.CONFLICT,
                    "job " + id + " is " + job.state() + ", only DEAD jobs can be replayed");
        }
        return repo.findById(id).map(j -> JobView.of(j, mapper)).orElseThrow();
    }
}
