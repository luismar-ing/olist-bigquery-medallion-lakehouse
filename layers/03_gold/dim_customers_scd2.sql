-- ============================================================
-- dim_customers — SCD Type 2 (carga inicial / reconstrucción histórica)
-- ============================================================
-- Natural key: customer_unique_id (customer_id es único POR ORDEN
-- en Olist, no por cliente real — es una medida de anonimización
-- documentada por el dataset, no un identificador de entidad).
--
-- Patrón "Gaps and Islands": en lugar de colapsar cada cliente a
-- una sola fila (lo que perdería el historial real de cambios de
-- ciudad ya presente en los datos crudos), se reconstruye cada
-- versión histórica comparando la ciudad de cada orden contra la
-- ciudad de la orden inmediatamente anterior del mismo cliente.
-- ============================================================

CREATE OR REPLACE TABLE gold_dw.dim_customers AS

WITH base AS (
  SELECT
    cc.customer_unique_id,
    co.order_purchase_timestamp,
    cc.customer_city,
    cc.customer_state,
    cc.customer_zip_code_prefix,
    LAG(cc.customer_city) OVER (
      PARTITION BY cc.customer_unique_id
      ORDER BY co.order_purchase_timestamp
    ) AS ciudad_anterior
  FROM silver_staging.clean_customers cc
  INNER JOIN silver_staging.clean_orders co
    USING (customer_id)
),

-- Flag: 1 marca el inicio de una nueva versión (primera orden del
-- cliente, o cambio real de ciudad respecto a la orden anterior)
flagged AS (
  SELECT *,
    CASE
      WHEN ciudad_anterior IS NULL OR ciudad_anterior != customer_city THEN 1
      ELSE 0
    END AS flag_cambio
  FROM base
),

-- "Islands": suma acumulada del flag -> agrupa las órdenes
-- consecutivas que pertenecen a la misma versión/tramo de ciudad
grouped AS (
  SELECT *,
    SUM(flag_cambio) OVER (
      PARTITION BY customer_unique_id
      ORDER BY order_purchase_timestamp
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS version_group
  FROM flagged
),

-- Colapsa cada isla a una sola fila: valid_from = fecha más
-- antigua de ese tramo de ciudad constante
collapsed AS (
  SELECT
    customer_unique_id,
    customer_city,
    customer_state,
    customer_zip_code_prefix,
    version_group,
    CAST(MIN(order_purchase_timestamp) AS DATE) AS valid_from
  FROM grouped
  GROUP BY customer_unique_id, customer_city, customer_state, customer_zip_code_prefix, version_group
),

-- valid_to de cada versión = valid_from de la siguiente versión
-- del mismo cliente. NULL en la última versión (vigente).
with_valid_to AS (
  SELECT *,
    LEAD(valid_from) OVER (
      PARTITION BY customer_unique_id
      ORDER BY version_group ASC
    ) AS valid_to
  FROM collapsed
)

SELECT
  ROW_NUMBER() OVER () AS customer_key,          -- surrogate key
  customer_unique_id,                             -- natural key
  customer_zip_code_prefix,
  customer_city,
  customer_state,
  valid_from,
  valid_to,
  (valid_to IS NULL) AS is_current
FROM with_valid_to;