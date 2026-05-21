CREATE OR REPLACE VIEW blocked_issues AS
SELECT
    i.*,
    (SELECT COUNT(*)
     FROM dependencies d
     JOIN issues blocker ON blocker.id = d.depends_on_issue_id
     WHERE d.issue_id = i.id
       AND d.type IN ('blocks', 'conditional-blocks', 'waits-for')
       AND blocker.status NOT IN ('closed', 'pinned')
    ) AS blocked_by_count
FROM issues i
WHERE i.is_blocked = 1
  AND i.status NOT IN ('closed', 'pinned');
