-- Chaos verification. Prints one row per check: name | ok | detail.
-- :expected_jobs is the total number of jobs created (initial N plus injected slow jobs).
WITH
j AS (SELECT count(*) AS total,
             count(*) FILTER (WHERE state IN ('PENDING','RUNNING')) AS unfinished,
             count(*) FILTER (WHERE state <> 'SUCCEEDED') AS not_succeeded,
             count(*) FILTER (WHERE state = 'DEAD') AS dead
      FROM jobs),
se AS (SELECT count(*) AS n FROM side_effects),
el AS (SELECT count(*) AS n, count(DISTINCT job_id) AS distinct_jobs FROM execution_log),
-- I5: the highest attempt in the execution log must equal the job's final attempt
i5a AS (
  SELECT count(*) AS bad FROM jobs jb
  JOIN (SELECT job_id, max(attempt) AS max_logged FROM execution_log GROUP BY job_id) l ON l.job_id = jb.id
  WHERE jb.attempt <> l.max_logged
),
-- I5: every completion the database accepted must have been for the job's final attempt
i5b AS (
  SELECT count(*) AS bad FROM acceptance_log a JOIN jobs jb ON jb.id = a.job_id WHERE a.attempt <> jb.attempt
),
-- I5: a job can be accepted as completed at most once
i5c AS (
  SELECT count(*) AS bad FROM (SELECT job_id FROM acceptance_log GROUP BY job_id HAVING count(*) > 1) d
),
audit AS (SELECT count(*) AS n FROM acceptance_log)
SELECT 'I1 no job left PENDING or RUNNING' AS check_name, (j.unfinished = 0) AS ok,
       j.unfinished || ' unfinished' AS detail FROM j
UNION ALL
SELECT 'I2 every job SUCCEEDED', (j.not_succeeded = 0), j.not_succeeded || ' not succeeded, ' || j.dead || ' dead' FROM j
UNION ALL
SELECT 'I3 side_effects has exactly N rows (nothing lost, nothing duplicated)',
       (se.n = :expected_jobs AND j.total = :expected_jobs),
       'side_effects=' || se.n || ' jobs=' || j.total || ' expected=' || :expected_jobs FROM se, j
UNION ALL
SELECT 'I4 execution_log has >= N rows (duplicates are expected, not a bug)',
       (el.n >= :expected_jobs AND el.distinct_jobs = :expected_jobs),
       'rows=' || el.n || ' distinct_jobs=' || el.distinct_jobs || ' duplicate_executions=' || (el.n - :expected_jobs) FROM el
UNION ALL
SELECT 'I5a final attempt == highest attempt in execution_log', (i5a.bad = 0), i5a.bad || ' jobs disagree' FROM i5a
UNION ALL
SELECT 'I5b every ACCEPTED completion was for the final attempt (no stale completion accepted)',
       (i5b.bad = 0), i5b.bad || ' stale completions accepted (audit rows=' || audit.n || ')' FROM i5b, audit
UNION ALL
SELECT 'I5c no job accepted as completed twice', (i5c.bad = 0), i5c.bad || ' jobs completed twice' FROM i5c;
