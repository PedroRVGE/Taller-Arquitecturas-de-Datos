"""Catálogo de entidades extraídas: columnas, tipos en Redshift y columna de avance (watermark).

Cada entidad define cómo se extrae de forma completa (full) e incremental:
- customers y products avanzan por updated_at: capturan altas y cambios (SCD tipo 2).
- orders, order_items, reviews y navigation_events avanzan por un identificador creciente:
  en estas fuentes los registros se insertan y no se modifican.
"""

from dataclasses import dataclass

# Metadatos que el extractor agrega a cada fila. _loaded_at lo pone Redshift (DEFAULT sysdate).
METADATA_COLUMNS = (
    ("_batch_id", "varchar(64)"),
    ("_extraction_mode", "varchar(16)"),
    ("_extracted_at", "timestamp"),
    ("_source", "varchar(64)"),
)


@dataclass(frozen=True)
class Entity:
    source: str  # carpeta en S3: mysql | documentdb
    name: str  # tabla o colección en la fuente
    raw_table: str  # tabla en el esquema raw de Redshift
    key: str  # llave de negocio
    columns: tuple  # ((nombre, tipo Redshift), ...)
    watermark_column: str
    watermark_kind: str  # timestamp | integer

    @property
    def column_names(self):
        return [name for name, _ in self.columns]

    @property
    def file_columns(self):
        """Columnas del CSV, en el orden exacto del COPY."""
        return self.column_names + [name for name, _ in METADATA_COLUMNS]

    @property
    def label(self):
        return f"{self.source}.{self.name}"


MYSQL_ENTITIES = (
    Entity(
        source="mysql",
        name="customers",
        raw_table="mysql_customers",
        key="customer_id",
        columns=(
            ("customer_id", "bigint"),
            ("name", "varchar(1024)"),
            ("email", "varchar(1024)"),
            ("phone", "varchar(256)"),
            ("city", "varchar(512)"),
            ("department", "varchar(512)"),
            ("registration_date", "date"),
            ("updated_at", "timestamp"),
        ),
        watermark_column="updated_at",
        watermark_kind="timestamp",
    ),
    Entity(
        source="mysql",
        name="products",
        raw_table="mysql_products",
        key="product_id",
        columns=(
            ("product_id", "bigint"),
            ("name", "varchar(1024)"),
            ("category", "varchar(512)"),
            ("price", "numeric(12,2)"),
            ("description", "varchar(4096)"),
            ("is_active", "boolean"),
            ("updated_at", "timestamp"),
        ),
        watermark_column="updated_at",
        watermark_kind="timestamp",
    ),
    Entity(
        source="mysql",
        name="orders",
        raw_table="mysql_orders",
        key="order_id",
        columns=(
            ("order_id", "bigint"),
            ("customer_id", "bigint"),
            ("order_date", "timestamp"),
            ("status", "varchar(128)"),
            ("sales_channel", "varchar(128)"),
            ("payment_method", "varchar(128)"),
            ("total", "numeric(14,2)"),
        ),
        watermark_column="order_id",
        watermark_kind="integer",
    ),
    Entity(
        source="mysql",
        name="order_items",
        raw_table="mysql_order_items",
        key="order_item_id",
        columns=(
            ("order_item_id", "bigint"),
            ("order_id", "bigint"),
            ("product_id", "bigint"),
            ("quantity", "integer"),
            ("unit_price", "numeric(12,2)"),
            ("subtotal", "numeric(14,2)"),
        ),
        # Las líneas llegan con su pedido: avanzan por order_id, no por order_item_id.
        watermark_column="order_id",
        watermark_kind="integer",
    ),
)

DOCUMENTDB_ENTITIES = (
    Entity(
        source="documentdb",
        name="reviews",
        raw_table="mongo_reviews",
        key="review_id",
        columns=(
            ("review_id", "bigint"),
            ("customer_id", "bigint"),
            ("product_id", "bigint"),
            ("rating", "integer"),
            ("reviewed_at", "timestamptz"),
            ("document_json", "varchar(65535)"),
        ),
        watermark_column="review_id",
        watermark_kind="integer",
    ),
    Entity(
        source="documentdb",
        name="navigation_events",
        raw_table="mongo_navigation_events",
        key="event_id",
        columns=(
            ("event_id", "bigint"),
            ("event_type", "varchar(256)"),
            ("occurred_at", "timestamptz"),
            ("customer_id", "bigint"),
            ("product_id", "bigint"),
            ("sales_channel", "varchar(128)"),
            ("document_json", "varchar(65535)"),
        ),
        watermark_column="event_id",
        watermark_kind="integer",
    ),
)

ALL_ENTITIES = MYSQL_ENTITIES + DOCUMENTDB_ENTITIES
ENTITIES_BY_LABEL = {entity.label: entity for entity in ALL_ENTITIES}
