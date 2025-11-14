{{ config(
    materialized='table',
    on_schema_change='ignore'
) }}

-- Tracking table for deferred revenue exports
-- This table persists which aggregated records have been exported and tracks the quantity exported
-- Records are uniquely identified by: payment_date, sku (item), class_code, payment_date
-- 
-- IMPORTANT: This is a persistent table that preserves manually inserted records.
-- After exporting agg_deferred_revenue "For Record", insert records directly into this table:
--
-- INSERT INTO public_fin_ref.stg_deferred_revenue_export_tracking 
--   (payment_date, item, class_code, payment_date, exported_quantity, exported_at)
-- SELECT payment_date, item, class_code, payment_date, quantity, current_timestamp
-- FROM public_fin_ref.agg_deferred_revenue
-- WHERE ... (your export criteria);
--
-- NOTE: This table tracks exported_quantity. If new orders come in for the same
-- payment_date/item/class_code/payment_date combination, only the difference between
-- current quantity and exported_quantity will appear in future exports.

select
    payment_date::date as payment_date,
    item::text as item,
    class_code::text as class_code,
    exported_quantity::numeric as exported_quantity,
    exported_at::timestamp as exported_at
from (
    -- Empty initial state - records will be inserted via SQL after exports
    -- This query returns no rows, creating an empty table structure
    select 
        null::date as payment_date,
        null::text as item,
        null::text as class_code,
        null::numeric as exported_quantity,
        null::timestamp as exported_at
    where 1=0
) as empty_initial

