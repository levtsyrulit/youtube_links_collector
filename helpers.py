import re


EMAIL_REGEX = re.compile(r"[^@]+@[^@]+\.[^@]+")

QUERY_WORKER_ID = """
    SELECT truck_id FROM cyber_workers WHERE email = %(email)s
"""

QUERY_TASK_ID = """
    WITH worker_task_history AS (
        SELECT
            BOOL_OR(is_review) FILTER (WHERE succeeded IS NOT NULL) as has_completed_review,
            BOOL_OR(NOT is_review) FILTER (WHERE succeeded IS NOT NULL) as has_completed_non_review
        FROM cyber_tasks
        WHERE
            worker_id = %(worker_id)s
        
        UNION ALL
        
        SELECT
            NULL as has_completed_review,
            NULL as has_completed_non_review
        LIMIT 1
    )

    SELECT ct.id
    FROM cyber_tasks ct
    JOIN worker_task_history wth ON (
        (ct.is_review = TRUE AND (wth.has_completed_review OR wth.has_completed_review IS NULL))
            OR
        (ct.is_review = FALSE AND (wth.has_completed_non_review OR NOT wth.has_completed_review OR wth.has_completed_review IS NULL))
    )
    WHERE
        ct.deleted_at IS NULL
        AND ct.succeeded IS NULL
        AND (ct.distributed_at < NOW() - INTERVAL '15 minutes' OR ct.distributed_at IS NULL)
    ORDER BY
        ct.created_at ASC
    LIMIT 1;
"""

QUERY_WORKER_ROLE="""
    SELECT
        CASE
            WHEN MAX(CASE WHEN is_review = TRUE THEN 1 ELSE 0 END) = 1 THEN 'reviewer'
            WHEN MAX(CASE WHEN is_review = FALSE THEN 1 ELSE 0 END) = 1 THEN 'labeler'
            ELSE NULL
        END as worker_role
    FROM cyber_tasks
    WHERE
        worker_id = %(worker_id)s AND succeeded IS NOT NULL;
"""

def is_email(email: str):
    return EMAIL_REGEX.match(email) is not None
