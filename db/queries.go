package db

const analysisIDByExternalIDQuery = `
	SELECT j.id
	FROM jobs j
	JOIN job_steps s ON s.job_id = j.id
	WHERE s.external_id = $1
`

const usernameQuery = `
	SELECT username
	FROM users
	WHERE id = $1;
`

const userIDQuery = `
	SELECT id
	FROM users
	WHERE username = $1;
`

const analysisQuery = `
	SELECT
		j.id,
		j.app_id,
		j.start_date,
		j.end_date,
		j.status,
		j.deleted,
		j.submission,
		j.user_id,
		j.subdomain,
		t.name job_type,
		t.system_id
	FROM jobs j
	JOIN job_types t ON j.job_type_id = job_types.id
	WHERE j.id = $1
	AND j.user_id = $2;
`

const currentCPUHoursForUserQuery = `
	SELECT 
		t.id,
		t.total,
		t.user_id,
		u.username,
		lower(t.effective_range) effective_start,
		upper(t.effective_range) effective_end,
		t.last_modified
	FROM cpu_usage_totals t
	JOIN users u ON t.user_id = u.id
	WHERE u.username = $1
	AND t.effective_range @> CURRENT_TIMESTAMP::timestamp
	LIMIT 1;
`

const addCurrentCPUHoursForUserStmt = `
	INSERT INTO cpu_usage_totals
		(total, user_id, effective_range)
	VALUES
		($1, $2, tsrange($3, $4, '[)'));
`

const updateCurrentCPUHoursForUserQuery = `
	UPDATE cpu_usage_totals
	SET total = $2
	WHERE user_id = $1
	AND effective_range @> CURRENT_TIMESTAMP::timestamp;
`

const allCPUHoursForUserQuery = `
	SELECT
		t.id,
		t.total,
		lower(t.effective_range) effective_start,
		upper(t.effective_range) effective_end,
		t.last_modified
	FROM cpu_usage_totals t
	JOIN users u ON t.user_id = u.id
	WHERE u.username = $1;
`

const currentCPUHoursQuery = `
	SELECT 
		t.id,
		t.total,
		t.user_id,
		u.username,
		lower(t.effective_range) effective_start,
		upper(t.effective_range) effective_end,
		t.last_modified
	FROM cpu_usage_totals t
	JOIN users u ON t.user_id = u.id
	WHERE t.effective_range @> CURRENT_TIMESTAMP::timestamp;
`

const allCPUHoursQuery = `
	SELECT 
		t.id,
		t.total,
		t.user_id,
		u.username,
		lower(t.effective_range) effective_start,
		upper(t.effective_range) effective_end,
		t.last_modified
	FROM cpu_usage_totals t
	JOIN users u ON t.user_id = u.id;
`

const insertCPUHourEventStmt = `
	INSERT INTO cpu_usage_events
		(record_date, effective_date, event_type_id, value, created_by) 
	VALUES 
		($1, $2, (SELECT id FROM cpu_usage_event_types WHERE name = $3), $4, $5);
`

const unprocessedEventsQuery = `
	SELECT 
		c.id,
		c.record_date,
		c.effective_date,
		e.name event_type,
		c.value,
		c.created_by,
		c.last_modified,
		c.claimed,
		c.claimed_by,
		c.claimed_on,
		c.claim_expires_on,
		c.processed,
		c.processing,
		c.processed_on,
		c.max_processing_attempts,
		c.attempts
	FROM cpu_usage_events c
	JOIN users u ON c.created_by = u.id
	JOIN cpu_usage_event_types e ON c.event_type_id = e.id
	WHERE NOT c.claimed
	AND NOT c.processed
	AND NOT c.processing
	AND c.attempts < c.max_processing_attempts
	AND CURRENT_TIMESTAMP >= COALESCE(c.claim_expires_on, to_timestamp(0));
`

const claimedByStmt = `
	UPDATE cpu_usage_events
	SET claimed = true,
		claimed_by = $2
	WHERE id = $1;
`

const processingStmt = `
	UPDATE cpu_usage_events
	SET processing = true,
		attempts = attempts + 1
	WHERE id = $1;
`

const finishedProcessingStmt = `
	UPDATE cpu_usage_events
	SET processing = false,
		processed = true
	WHERE id = $1;
`

const registerWorkerStmt = `
	INSERT INTO cpu_usage_workers
		(name, activation_expires_on)
	VALUES
		($1, $2)
	RETURNING id;
`

const unregisterWorkerStmt = `
	UPDATE cpu_usage_workers
	SET active = false,
		getting_work = false
	WHERE id = $1;
`

const refreshWorkerStmt = `
	UPDATE cpu_usage_workers
	SET activation_expires_on = $2
	WHERE id = $1;
`

// Only purge workers (set their activation flag to false) if they're not getting work,
// they're not actively working on something, and the activation timestamp has passed.
const purgeExpiredWorkersStmt = `
	UPDATE cpu_usage_workers
	SET active = false,
		activation_expires_on = NULL
	WHERE active
	AND NOT getting_work
	AND NOT working
	AND CURRENT_TIMESTAMP >= COALESCE(activation_expires_on, to_timestamp(0));
`

// Only purge work seekers if the expiration date on their search has expired.
const purgeExpiredWorkSeekersStmt = `
	UPDATE cpu_usage_workers
	SET getting_work = false,
		getting_work_on = NULL,
		getting_work_expires_on = NULL
	WHERE active
	AND getting_work
	AND NOT working
	AND CURRENT_TIMESTAMP >= COALESCE(getting_work_expires_on, to_timestamp(0));
`

const purgeExpiredWorkClaimsStmt = `
	UPDATE cpu_usage_events
	SET claimed = false,
		claimed_by = NULL,
		claimed_on = NULL
	WHERE claimed = true
	AND processing = false
	AND processed = false
	AND CURRENT_TIMESTAMP >= COALESCE(claim_expires_on, to_timestamp(0));
`

const resetWorkClaimForInactiveWorkersStmt = `
	UPDATE cpu_usage_events
	SET claimed = false,
		claimed_by = NULL,
		claimed_on = NULL
	FROM ( SELECT id FROM cpu_usage_workers WHERE NOT active ) AS sub
	WHERE claimed = true
	AND claimed_by = sub.id;
`

const gettingWorkStmt = `
	UPDATE cpu_usage_workers
	SET getting_work = true,
		getting_work_expires_on = $2
	WHERE id = $1;
`

const notGettingWorkStmt = `
	UPDATE cpu_usage_workers
	SET getting_work = false,
		getting_work_expires_on = NULL
	WHERE id = $1;
`

const setWorkingStmt = `
	UPDATE cpu_usage_workers
	SET working = $2
	WHERE id = $1;
`
