-- Estado vigente de líneas de pedido entre los lotes de raw.
select
    cast(order_item_id as bigint) as order_item_id,
    cast(order_id as bigint) as order_id,
    cast(product_id as bigint) as product_id,
    cast(quantity as integer) as quantity,
    cast(unit_price as decimal(12, 2)) as unit_price,
    cast(subtotal as decimal(14, 2)) as subtotal,
    _batch_id,
    _extracted_at,
    _loaded_at,
    _source
from ({{ latest_batch_rows(source('mysql_raw', 'mysql_order_items'), 'order_item_id') }}) as current_rows
