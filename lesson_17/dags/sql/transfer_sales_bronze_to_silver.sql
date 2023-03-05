DELETE FROM `{{ params.project_id }}.silver.sales`
WHERE DATE(purchase_date) = '{{ ds }}'
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
    CAST(PurchaseDate AS DATE)      AS purchase_date,
    Product                         AS product,
    CAST(RTRIM(RTRIM(
        Price,
        'USD'), '$')
    AS NUMERIC)                     AS price,
    CASE
        WHEN ENDS_WITH(Price,'$') THEN 'USD'
        WHEN ENDS_WITH(Price,'USD') THEN 'USD'
        ELSE ''
    END                             AS currency,

    _id,
    _logical_dt,
    _job_start_dt
FROM `{{ params.project_id }}.bronze.sales`
WHERE DATE(_logical_dt) = CAST(CAST('{{ ds }}' AS TIMESTAMP) AS DATE)
;