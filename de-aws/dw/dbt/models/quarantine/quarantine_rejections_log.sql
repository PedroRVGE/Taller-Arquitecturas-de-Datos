{{ config(
    materialized='incremental',
    unique_key='rejection_sk',
    incremental_strategy='delete+insert'
) }}

-- Historial de rechazos: una fila por registro rechazado, motivo y lote de origen.
-- Las tablas quarantine_* muestran el estado actual; este log conserva los lotes anteriores.
-- Solo se agregan rechazos nuevos: detected_in_batch_id y recorded_at guardan la primera detección.
{% set rejected = [
    ('mysql', 'customers', 'customer_id', 'quarantine_mysql__customers'),
    ('mysql', 'products', 'product_id', 'quarantine_mysql__products'),
    ('mysql', 'orders', 'order_id', 'quarantine_mysql__orders'),
    ('mysql', 'order_items', 'order_item_id', 'quarantine_mysql__order_items'),
    ('documentdb', 'reviews', 'review_id', 'quarantine_mongodb__reviews'),
    ('documentdb', 'navigation_events', 'event_id', 'quarantine_mongodb__navigation_events'),
] %}

with rejections as (
    {% for source_name, entity, key, model in rejected %}
    select
        cast('{{ source_name }}' as varchar(32)) as source_name,
        cast('{{ entity }}' as varchar(64)) as entity,
        cast({{ key }} as varchar(64)) as record_key,
        cast(rejection_reason as varchar(64)) as rejection_reason,
        cast(_batch_id as varchar(64)) as source_batch_id,
        _extracted_at as source_extracted_at
    from {{ ref(model) }}
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
)

select
    {{ dbt_utils.generate_surrogate_key(['source_name', 'entity', 'record_key', 'source_batch_id', 'rejection_reason']) }} as rejection_sk,
    source_name,
    entity,
    record_key,
    rejection_reason,
    source_batch_id,
    source_extracted_at,
    cast('{{ var("batch_id") }}' as varchar(64)) as detected_in_batch_id,
    getdate() as recorded_at
from rejections
{% if is_incremental() %}
where {{ dbt_utils.generate_surrogate_key(['source_name', 'entity', 'record_key', 'source_batch_id', 'rejection_reason']) }}
    not in (select rejection_sk from {{ this }})
{% endif %}
