DECLARE v_dag_logical_date DEFAULT CAST(CAST('{{ dag_run.logical_date }}' AS TIMESTAMP) AS DATE);
DECLARE v_dag_start_date DEFAULT CAST('{{ dag_run.start_date }}' AS TIMESTAMP);
DECLARE v_dag_ds DEFAULT CAST(CAST('{{ ds }}' AS TIMESTAMP) AS DATE);

DELETE FROM `{{ params.project_id }}.bronze.sales`
WHERE _logical_dt = v_dag_ds
;

INSERT `{{ params.project_id }}.bronze.sales` (
    CustomerId,
    PurchaseDate,
    Product,
    Price,

    _id,
    _logical_dt,
    _job_start_dt
)
SELECT
    CustomerId,
    PurchaseDate,
    Product,
    Price,

    GENERATE_UUID() AS _id,
    v_dag_logical_date AS _logical_dt,
    v_dag_start_date AS _job_start_dt
FROM sales_csv
;