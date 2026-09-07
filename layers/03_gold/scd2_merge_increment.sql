-- ============================================================
-- SCD Type 2 — Carga incremental (MERGE + INSERT transaccional)
-- ============================================================
-- Un solo cambio de cliente requiere DOS operaciones:
--   1) Cerrar la versión vigente (UPDATE: is_current=FALSE, valid_to=fecha)
--   2) Insertar la nueva versión (INSERT: is_current=TRUE, valid_from=fecha)
-- BigQuery MERGE no puede hacer ambas cosas sobre la misma fila
-- coincidente, por lo que se ejecutan como dos statements dentro
-- de una misma transacción explícita, garantizando atomicidad:
-- o se aplican ambos, o no se aplica ninguno.
-- ============================================================

-- Tabla de staging que simula cambios entrantes desde un sistema
-- origen (ej. actualización de perfil del cliente en un OLTP).
CREATE TABLE IF NOT EXISTS gold_dw.stg_customer_changes (
  customer_unique_id STRING,
  customer_zip_code_prefix STRING,
  customer_city STRING,
  customer_state STRING,
  change_date DATE
);

BEGIN
  BEGIN TRANSACTION;

  -- Paso 1: cerrar versiones vigentes que cambiaron, e insertar
  -- clientes completamente nuevos que no existían todavía.
  MERGE gold_dw.dim_customers AS target
  USING (
    SELECT *,
      (SELECT COALESCE(MAX(customer_key), 0) FROM gold_dw.dim_customers)
        + ROW_NUMBER() OVER () AS new_customer_key
    FROM gold_dw.stg_customer_changes
  ) AS source
  ON target.customer_unique_id = source.customer_unique_id
     AND target.is_current = TRUE

  WHEN MATCHED AND (
        target.customer_city != source.customer_city
        OR target.customer_state != source.customer_state
        OR target.customer_zip_code_prefix != source.customer_zip_code_prefix
      )
  THEN UPDATE SET
    target.valid_to = source.change_date,
    target.is_current = FALSE

  WHEN NOT MATCHED BY TARGET
  THEN INSERT (
    customer_key, customer_unique_id, customer_zip_code_prefix,
    customer_city, customer_state, valid_from, valid_to, is_current
  )
  VALUES (
    new_customer_key,
    source.customer_unique_id, source.customer_zip_code_prefix,
    source.customer_city, source.customer_state,
    source.change_date, NULL, TRUE
  );

  -- Paso 2: insertar la versión nueva de los clientes que sí
  -- cambiaron (identificados por el valid_to que el MERGE acaba
  -- de asignar en este mismo batch — no cualquier fila histórica
  -- cerrada en el pasado).
  INSERT INTO gold_dw.dim_customers (
    customer_key, customer_unique_id, customer_zip_code_prefix,
    customer_city, customer_state, valid_from, valid_to, is_current
  )
  SELECT
    (SELECT MAX(customer_key) FROM gold_dw.dim_customers) + ROW_NUMBER() OVER (),
    scc.customer_unique_id, scc.customer_zip_code_prefix,
    scc.customer_city, scc.customer_state,
    scc.change_date, CAST(NULL AS DATE), TRUE
  FROM gold_dw.stg_customer_changes scc
  INNER JOIN gold_dw.dim_customers dc
    ON scc.customer_unique_id = dc.customer_unique_id
  WHERE dc.is_current = FALSE AND dc.valid_to = scc.change_date;

  COMMIT TRANSACTION;

EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = FORMAT('SCD Type 2 MERGE failed, ROLLBACK: %s', @@error.message);
END;