"""
Bronze Layer Ingestion — Olist E-Commerce Dataset
---------------------------------------------------
Carga las 5 tablas fuente de Olist (CSV) hacia BigQuery `bronze_raw`
sin ninguna transformación de negocio. Regla de Bronze: todo se
carga como STRING, se agrega metadata técnica de ingesta para
trazabilidad, y se valida el conteo de filas contra el CSV origen.
"""

import os
from datetime import datetime, timezone

import pandas as pd
from google.cloud import bigquery

# ---- CONFIGURACIÓN ----
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_BRONZE = os.getenv("DATASET_BRONZE")
CSV_FOLDER = os.getenv("CSV_FOLDER")

# Solo las 5 tablas usadas en el modelo dimensional (Proyecto 1 / Proyecto 2)
CSV_FILES = [
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_customers_dataset.csv",
    "olist_products_dataset.csv",
    "olist_order_payments_dataset.csv",
]

client = bigquery.Client(project=PROJECT_ID)

for csv_file in CSV_FILES:
    file_path = os.path.join(CSV_FOLDER, csv_file)
    table_name = csv_file.replace(".csv", "")
    table_id = f"{PROJECT_ID}.{DATASET_BRONZE}.{table_name}"

    print(f"Procesando: {csv_file} -> {table_id}")

    # Bronze discipline: todo se lee y carga como STRING, sin inferencia
    # de tipos. La conversión de tipos es responsabilidad de la capa Silver.
    df = pd.read_csv(file_path, dtype=str)

    # Metadata técnica de ingesta (lineage / auditoría)
    df["_ingested_at"] = datetime.now(timezone.utc)
    df["_source_file"] = csv_file

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
    )

    load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    load_job.result()  # espera a que el job asíncrono termine

    # Validación: filas del CSV origen vs. filas cargadas en BigQuery
    table = client.get_table(table_id)
    print(f"  CSV: {len(df)} filas | BigQuery: {table.num_rows} filas")
    assert len(df) == table.num_rows, f"¡Descuadre en {table_name}!"

print("Carga a Bronze completada.")