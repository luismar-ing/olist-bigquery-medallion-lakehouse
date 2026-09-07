-- ============================================================
-- dim_products — SCD Type 0/1
-- ============================================================
-- Los atributos de producto son estáticos para el propósito de
-- este análisis: si un dato cambia (ej. corrección de categoría),
-- no aporta valor de negocio mantener versiones históricas.
--
-- No se genera un surrogate key: bajo SCD Type 1 nunca existe
-- más de una fila vigente por product_id (no hay historial de
-- versiones que desambiguar), por lo que product_id puede
-- cumplir directamente el rol de llave hacia fact_sales.
-- ============================================================

CREATE OR REPLACE TABLE gold_dw.dim_products AS
SELECT
  product_id,
  product_category_name,
  product_weight_g,
  product_length_cm,
  product_height_cm,
  product_width_cm
FROM silver_staging.clean_products;