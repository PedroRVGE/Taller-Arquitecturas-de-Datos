{# Equivalentes en Redshift de funciones que la versión local usaba en DuckDB. #}

{# Redshift no soporta IS DISTINCT FROM: comparación que trata NULL como un valor. #}
{% macro is_distinct_from(left, right) -%}
(({{ left }}) <> ({{ right }}) or (({{ left }}) is null) <> (({{ right }}) is null))
{%- endmacro %}

{# DuckDB: timezone('America/Bogota', ts). Redshift: timestamptz AT TIME ZONE devuelve la hora local
   como timestamp; convert_timezone no acepta timestamptz y los instantes de MongoDB llegan con ese tipo. #}
{% macro to_business_time(timestamp_expression) -%}
(({{ timestamp_expression }}) at time zone '{{ var("business_timezone") }}')
{%- endmacro %}

{# Estado actual de una tabla raw que acumula lotes.

   Raw guarda todas las extracciones (una por lote) con _batch_id y _extracted_at.
   El estado vigente de la fuente es: la última extracción completa (full) más las
   incrementales posteriores, quedándose con la versión más reciente de cada llave.
   Las filas que trae cada lote viajan con su _batch_id hasta quarantine. #}
{% macro latest_batch_rows(source_relation, key_column) %}
select *
from (
    select
        raw_rows.*,
        row_number() over (
            partition by raw_rows.{{ key_column }}
            order by raw_rows._extracted_at desc, raw_rows._loaded_at desc
        ) as _version_rank
    from {{ source_relation }} as raw_rows
    where raw_rows._extracted_at >= (
        select coalesce(max(full_batches._extracted_at), cast('1900-01-01' as timestamp))
        from {{ source_relation }} as full_batches
        where full_batches._extraction_mode = 'full'
    )
) as ranked
where _version_rank = 1
{% endmacro %}

{# Día de la semana ISO (lunes = 1 ... domingo = 7). Redshift solo ofrece dow (domingo = 0). #}
{% macro iso_day_of_week(date_expression) -%}
cast(case extract(dow from {{ date_expression }}) when 0 then 7 else extract(dow from {{ date_expression }}) end as integer)
{%- endmacro %}
