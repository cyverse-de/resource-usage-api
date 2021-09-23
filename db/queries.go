package db

const analysisIDByExternalIDQuery = `
	SELECT j.id
	  FROM jobs j
	  JOIN job_steps s ON s.job_id = j.id
	 WHERE s.external_id = $1
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
	WHERE j.id = %1
	AND j.user_id = %2;
`

const currentCPUHoursForUserQuery = `
	SELECT 
		t.id,
		t.total,
		lower(t.effective_range) effective_start,
		upper(t.effective_range) effective_end,
		t.last_modified
	FROM cpu_usage_totals t
	JOIN users u ON t.user_id = u.id
	WHERE u.username = %1
	AND t.effective_range @> now()
	LIMIT 1;
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
	WHERE u.username = %1;
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
	WHERE t.effective_range @> now();
`

const allCPUHoursQuery = `
	SELECT 
		t.id,
		t.total,
		t.user_id,
		t.username,
		lower(t.effective_range) effective_start,
		upper(t.effective_range) effective_end,
		t.last_modified
	FROM cpu_usage_totals t
	JOIN users u ON t.user_id = u.id;
`

const insertCPUHourEvent = `
	INSERT INTO cpu_usage_events
		(record_date, effective_date, event_type_id, value, created_by) 
	VALUES 
		(%1, %2, (SELECT id FROM cpu_usage_event_types WHERE name = %3), %4, %5);
`
