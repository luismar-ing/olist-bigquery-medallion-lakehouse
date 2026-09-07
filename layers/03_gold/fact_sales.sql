-- ============================================================
-- fact_sales — Transaction fact table
-- ============================================================
-- Grano: una fila = un item dentro de una orden.
-- Particionada por fecha del evento (order_purchase_date) para
-- partition pruning en queries por rango de fechas.
-- Clustered por las surrogate/natural keys de mayor uso en
-- filtros analíticos (customer_key, product_id).
--
-- order_id vive como columna directa (degenerate dimension):
-- no tiene atributos descriptivos propios que justifiquen una
-- tabla dimensión separada.
--
-- El JOIN contra dim_customers resuelve el customer_key vigente
-- en la fecha EXACTA de cada orden (rango semiabierto
-- [valid_from, valid_to) para que cada fecha pertenezca a
-- exactamente una versión, nunca a cero ni a dos).
-- ============================================================

CREATE OR REPLACE TABLE gold_dw.fact_sales
PARTITION BY order_purchase_date
CLUSTER BY customer_key, product_id
AS

WITH prep_clean_orders AS (
  SELECT
    co.order_id,
    dc.customer_key,
    co.order_status,
    co.order_purchase_timestamp,
    co.order_delivered_customer_date,
    co.order_estimated_delivery_date
  FROM silver_staging.clean_orders co
  INNER JOIN silver_staging.clean_customers cc
    ON co.customer_id = cc.customer_id
  INNER JOIN gold_dw.dim_customers dc
    ON cc.customer_unique_id = dc.customer_unique_id
  WHERE CAST(co.order_purchase_timestamp AS DATE) >= dc.valid_from
    AND (CAST(co.order_purchase_timestamp AS DATE) < dc.valid_to OR dc.valid_to IS NULL)
),

-- Pre-agregación de payments a grano "una fila por orden" para
-- evitar fan-out: una orden puede tener múltiples métodos de pago
-- (ej. tarjeta + voucher) en Olist.
prep_clean_order_payments AS (
  SELECT
    order_id,
    SUM(payment_installments) OVER (PARTITION BY order_id) AS total_payment_installments
  FROM silver_staging.clean_order_payments
  QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id) = 1
),

cte_without_metrics AS (
  SELECT
    CONCAT(pco.order_id, '-', coi.order_item_id) AS fact_id,
    pco.order_id,
    coi.order_item_id,
    pco.customer_key,
    dp.product_id,
    pco.order_status,
    CAST(pco.order_purchase_timestamp AS DATE) AS order_purchase_date,
    CAST(pco.order_delivered_customer_date AS DATE) AS order_delivered_date,
    pcop.total_payment_installments,
    coi.freight_value,
    coi.price AS product_unit_price
  FROM prep_clean_orders pco
  INNER JOIN silver_staging.clean_order_items coi
    ON pco.order_id = coi.order_id
  INNER JOIN gold_dw.dim_products dp
    ON coi.product_id = dp.product_id
  INNER JOIN prep_clean_order_payments pcop
    ON pco.order_id = pcop.order_id
),

cte_metrics AS (
  SELECT
    cwe.*,
    COUNT(cwe.product_id) OVER (PARTITION BY cwe.order_id, cwe.product_id) AS order_product_qty,
    SUM(cwe.product_unit_price) OVER (PARTITION BY cwe.order_id, cwe.product_id) AS order_product_total_sale,
    SUM(cwe.product_unit_price) OVER (PARTITION BY cwe.order_id) AS order_products_total_sale,
    SUM(cwe.freight_value) OVER (PARTITION BY cwe.order_id) AS order_freight_value
  FROM cte_without_metrics cwe
)

SELECT
  fact_id,
  order_id,
  order_item_id,
  customer_key,
  product_id,
  order_status,
  order_purchase_date,
  order_delivered_date,
  total_payment_installments,
  freight_value,
  product_unit_price,
  order_product_qty,
  order_product_total_sale,
  order_products_total_sale,
  order_freight_value,
  order_products_total_sale + order_freight_value AS order_total_sale
FROM cte_metrics;