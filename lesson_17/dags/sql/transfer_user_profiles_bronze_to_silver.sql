DECLARE v_dag_logical_date DEFAULT CAST(CAST('{{ dag_run.logical_date }}' AS TIMESTAMP) AS DATE);
DECLARE v_dag_start_date DEFAULT CAST('{{ dag_run.start_date }}' AS TIMESTAMP);
DECLARE v_dag_ds DEFAULT CAST(CAST('{{ ds }}' AS TIMESTAMP) AS DATE);

TRUNCATE TABLE `{{ params.project_id }}.silver.user_profiles`
;

INSERT INTO `{{ params.project_id }}.silver.user_profiles` (
    email,
    full_name,
    state,
    birth_date,
    phone_number,

    full_name_parts,

    _id,
    _logical_dt,
    _job_start_dt
)
SELECT
    TRIM(email),
    TRIM(full_name),
    TRIM(state),
    CAST(TRIM(birth_date) as DATE),
    TRIM(phone_number),

    SPLIT(TRIM(full_name), ' ') as full_name_parts,

    _id,
    _logical_dt,
    _job_start_dt
FROM `{{ params.project_id }}.bronze.user_profiles`
;