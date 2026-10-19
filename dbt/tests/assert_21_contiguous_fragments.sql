-- Singular test: dbt FAILS this test if the query returns any rows.
-- It returns a row unless order_no is exactly 1, 2, 3, ..., 21 (no gaps, no extras).
select
    count(*)      as n_fragments,
    min(order_no) as first_no,
    max(order_no) as last_no
from {{ ref('stg_fragments') }}
having count(*) != {{ var('expected_fragments') }}
    or count(distinct order_no) != {{ var('expected_fragments') }}
    or min(order_no) != 1
    or max(order_no) != {{ var('expected_fragments') }}
