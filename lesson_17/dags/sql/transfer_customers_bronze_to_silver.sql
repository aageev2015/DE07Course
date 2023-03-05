TRUNCATE TABLE `{{ params.project_id }}.silver.customers`
;

INSERT `{{ params.project_id }}.silver.customers` (
    client_id,
    first_name,
    last_name,
    email,
    registration_date,
    state,

    _id,
    _logical_dt,
    _job_start_dt
)
SELECT
    CAST(Id as INT64),
    FirstName,
    LastName,
    Email,
    CAST(RegistrationDate AS DATE),
    State,

    _id,
    _logical_dt,
    _job_start_dt
FROM `{{ params.project_id }}.bronze.customers`
;