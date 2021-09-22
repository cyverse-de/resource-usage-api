package db

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
