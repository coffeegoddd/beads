CREATE OR REPLACE VIEW ready_issues AS
SELECT i.*
FROM issues i
WHERE (
    i.status = 'open'
    OR i.status IN (SELECT name FROM custom_statuses WHERE category = 'active')
  )
  AND i.is_blocked = 0
  AND (i.ephemeral = 0 OR i.ephemeral IS NULL)
  AND (i.defer_until IS NULL OR i.defer_until <= UTC_TIMESTAMP())
  AND NOT EXISTS (
    SELECT 1 FROM dependencies d_parent
    JOIN issues parent ON parent.id = d_parent.depends_on_issue_id
    WHERE d_parent.issue_id = i.id
      AND d_parent.type = 'parent-child'
      AND parent.defer_until IS NOT NULL
      AND parent.defer_until > UTC_TIMESTAMP()
  );
