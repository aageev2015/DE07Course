DELETE FROM `{{ params.project_id }}.bronze.sales`
WHERE DATE(_logical_dt) = '{{ ds }}'
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
    CAST(CAST('{{ dag_run.logical_date }}' AS TIMESTAMP) AS DATE) AS _logical_dt,
    CAST('{{ dag_run.start_date }}' AS TIMESTAMP) AS _job_start_dt
FROM sales_csv
;