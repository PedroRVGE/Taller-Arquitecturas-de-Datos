# Dashboard en QuickSight

El dashboard consume la vista `marts.rpt_sales_dashboard`: una fila por línea de venta con fecha, categoría, territorio, canal, cantidad, subtotal y `data_refreshed_at` (último `_loaded_at` de las líneas en raw).

## Preparación (una vez)

1. Suscribirse a QuickSight en la misma región que Redshift (`us-east-1`).
2. Conectividad: con `redshift_publicly_accessible = true` (por defecto) el workgroup tiene endpoint público y su security group solo acepta el rango IP de QuickSight de la región (`quicksight_cidrs`). Para dejar Redshift privado use `false` y cree una *VPC connection* en QuickSight Enterprise.
3. Fuente de datos:
   - Con Terraform: defina `quicksight_principal_arn` y ejecute `terraform apply`; crea `aws_quicksight_data_source.redshift`.
   - A mano: *Datasets → New dataset → Redshift (manual connect)* con el endpoint de `terraform output redshift_endpoint`, puerto 5439, base `ecommerce_dw` y el usuario de Redshift del secreto.

## Dataset y visuales

- Dataset: tabla `marts.rpt_sales_dashboard`. Modo **Direct Query** (consulta Redshift en cada visual) o **SPICE** (copia en memoria).
- Visual 1, línea o barras: `sale_date` (por mes) contra `sum(subtotal)`, con `category` como color.
- Visual 2, KPI: `sum(subtotal)` con el título "Ventas totales".
- Filtro: `category` o `department` (control de lista) aplicado a todos los visuales.
- Publicar como dashboard.

## Consulta de validación del indicador

El KPI debe coincidir con esta consulta usando los mismos filtros. Ejemplo con el filtro `category = 'Electrónica'`:

```sql
select
    round(sum(subtotal), 2) as ventas_totales,
    count(*) as lineas,
    max(data_refreshed_at) as datos_actualizados_a
from marts.rpt_sales_dashboard
where category = 'Electrónica';
```

`datos_actualizados_a` debe ser la misma fecha que muestra el dashboard (añada `data_refreshed_at` como visual de tabla o en el subtítulo). Si difiere, el dataset SPICE no se ha refrescado después del último lote.

## Actualización con SPICE

SPICE no ve los lotes nuevos hasta refrescar el dataset:

- Programado: *Dataset → Refresh → Schedule* diario a las 07:00 (después del `dw_pipeline` de las 06:00).
- Inmediato tras un lote: *Refresh now*, o por API:

```bash
aws quicksight create-ingestion --aws-account-id <cuenta> --data-set-id <dataset-id> \
  --ingestion-id "lote-$(date +%Y%m%d%H%M%S)"
```

Con Direct Query no hace falta refrescar: cada visual consulta Redshift.
