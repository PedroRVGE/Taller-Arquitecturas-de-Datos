{{ config(materialized='view') }}

-- Vista plana para QuickSight: una fila por línea de venta con sus atributos de dimensión.
-- QuickSight consume esta vista; la consulta de validación del dashboard usa los mismos filtros.
select
    sales.sale_sk,
    calendar.date as sale_date,
    calendar.year,
    calendar.month,
    product.category,
    product.name as product_name,
    geography.department,
    geography.city,
    channel.sales_channel,
    sales.quantity,
    sales.unit_price,
    sales.subtotal,
    refresh.data_refreshed_at
from {{ ref('fct_sales') }} as sales
inner join {{ ref('dim_date') }} as calendar on calendar.date_sk = sales.date_sk
inner join {{ ref('dim_product') }} as product on product.product_sk = sales.product_sk
inner join {{ ref('dim_geography') }} as geography on geography.geography_sk = sales.geography_sk
inner join {{ ref('dim_sales_channel') }} as channel on channel.sales_channel_sk = sales.sales_channel_sk
-- Fecha de los datos: última extracción desde la fuente de las líneas válidas. Se usa _extracted_at
-- (viene en el archivo del lote) y no _loaded_at, que cambia al reprocesar un lote sin datos nuevos.
cross join (
    select max(_extracted_at) as data_refreshed_at
    from {{ ref('int_mysql__order_items__valid') }}
) as refresh
