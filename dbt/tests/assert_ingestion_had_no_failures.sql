-- The most recent ingestion run must have loaded every dataset successfully.
-- Transformations running on a partial load is the silent-failure mode this whole
-- pipeline is designed to prevent.
-- Severity: error.

with latest_run as (
    select run_id
    from {{ source('raw', '_load_log') }}
    order by loaded_at desc
    limit 1
)

select l.*
from {{ source('raw', '_load_log') }} l
join latest_run r on r.run_id = l.run_id
where l.status <> 'OK'
