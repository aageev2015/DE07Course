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
    CAST(CAST('{{ dag_run.logical_date }}' AS TIMESTAMP) AS DATE) AS _logical_dt,
    CAST('{{ dag_run.start_date }}' AS TIMESTAMP) AS _job_start_dt
FROM deduplicated
;