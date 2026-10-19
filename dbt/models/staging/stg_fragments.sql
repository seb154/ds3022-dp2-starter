-- Clean and type the raw fragments.
-- "distinct" makes this safe if the same fragment was stored twice.
select distinct
    cast(order_no as integer) as order_no,   -- "7" (text) -> 7 (number)
    trim(word)                as word        -- remove stray spaces
from {{ source('raw', 'fragments') }}
