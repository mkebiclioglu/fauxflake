-- the source's text has to be one of the names the list holds, or no row finds its quarter
select e.event_id, q.quarter_name, q.starts_on
from {{ ref('stg_events') }} as e
inner join {{ ref('quarters') }} as q
    on lower(e.fiscal_quarter) = lower(q.quarter_name)
