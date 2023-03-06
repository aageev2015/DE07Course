DECLARE v_dag_logical_date DEFAULT CAST(CAST('{{ dag_run.logical_date }}' AS TIMESTAMP) AS DATE);
DECLARE v_dag_start_date DEFAULT CAST('{{ dag_run.start_date }}' AS TIMESTAMP);
DECLARE v_dag_ds DEFAULT CAST(CAST('{{ ds }}' AS TIMESTAMP) AS DATE);

DELETE FROM `{{ params.project_id }}.silver.sales`
WHERE DATE(purchase_date) = v_dag_ds
;

INSERT `{{ params.project_id }}.silver.sales` (
    client_id,
    purchase_date,
    product,
    price,
    currency,

    _id,
    _logical_dt,
    _job_start_dt
)
SELECT
    CAST(CustomerId AS INT64)       AS client_id,
    CAST(REPLACE(REPLACE(
            TRIM(PurchaseDate)
        , '/', '-')
        , '\\', '-')
        AS DATE)                    AS purchase_date,
    TRIM(Product)                   AS product,
    CAST(RTRIM(RTRIM(
        TRIM(Price),
        'USD'), '$')
    AS NUMERIC)                     AS price,
    CASE
        WHEN ENDS_WITH(TRIM(Price),'$') THEN 'USD'
        WHEN ENDS_WITH(TRIM(Price),'USD') THEN 'USD'
        ELSE ''
    END                             AS currency,

    _id,
    _logical_dt,
    _job_start_dt
FROM `{{ params.project_id }}.bronze.sales`
WHERE DATE(_logical_dt) = v_dag_ds
;