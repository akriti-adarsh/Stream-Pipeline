-- One view per source table, renames and casts only, no business logic.
select
    txn_id,
    ride_id,
    amount_cents::bigint as amount_cents,
    status,
    method,
    ts as paid_at,
    ingested_at
from {{ source('raw', 'payments_transactions') }}
