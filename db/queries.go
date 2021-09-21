package db

const currentCPUHoursForUserQuery = `
	SELECT 
		t.id,
		t.total,
		t.effective_range,
		t.last_modified
	FROM cpu_usage_totals t
	JOIN users u ON t.user_id = u.id
	WHERE u.username = %1
	AND t.effective_range @> now();
`

const allCPUHoursForUserQuery = `
	SELECT
		t.id,
		t.total,
		t.effective_range,
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
		t.effective_range,
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
		t.effective_range,
		t.last_modified
	FROM cpu_usage_totals t
	JOIN users u ON t.user_id = u.id;
`
