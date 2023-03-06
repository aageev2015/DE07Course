DECLARE v_dag_logical_date DEFAULT CAST(CAST('{{ dag_run.logical_date }}' AS TIMESTAMP) AS DATE);
DECLARE v_dag_start_date DEFAULT CAST('{{ dag_run.start_date }}' AS TIMESTAMP);
DECLARE v_dag_ds DEFAULT CAST(CAST('{{ ds }}' AS TIMESTAMP) AS DATE);

TRUNCATE TABLE `{{ params.project_id }}.bronze.customers`
;

INSERT `{{ params.project_id }}.bronze.customers` (
    Id,
    FirstName,
    LastName,
    Email,
    RegistrationDate,
    State,

    _id,
    _logical_dt,
    _job_start_dt
)
WITH deduplicated as (
    SELECT DISTINCT
        Id,
        FirstName,
        LastName,
        Email,
        RegistrationDate,
        State
    FROM customers_csv
)
SELECT
    Id,
    FirstName,
    LastName,
    Email,
    RegistrationDate,
    State,

    GENERATE_UUID() AS _id,
    v_dag_logical_date AS _logical_dt,
    v_dag_start_date AS _job_start_dt
FROM deduplicated
;