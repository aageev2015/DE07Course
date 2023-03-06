DECLARE v_dag_logical_date DEFAULT CAST(CAST('{{ dag_run.logical_date }}' AS TIMESTAMP) AS DATE);
DECLARE v_dag_start_date DEFAULT CAST('{{ dag_run.start_date }}' AS TIMESTAMP);
DECLARE v_dag_ds DEFAULT CAST(CAST('{{ ds }}' AS TIMESTAMP) AS DATE);

TRUNCATE TABLE `{{ params.project_id }}.gold.user_profiles_enriched`
;

INSERT INTO `{{ params.project_id }}.gold.user_profiles_enriched` (
    client_id,
    first_name,
    last_name,
    email,
    registration_date,
    state,
    birth_date,
    age,
    phone_number,

    _id,
    _logical_dt,
    _job_start_dt
)
SELECT
    customers.client_id                                         as client_id,
    IFNULL(
        IF(ARRAY_LENGTH(user_profiles.full_name_parts)>=1, user_profiles.full_name_parts[offset(0)], NULL),
        customers.first_name)                                   as first_name,
    IFNULL(
        IF(ARRAY_LENGTH(user_profiles.full_name_parts)>=2, user_profiles.full_name_parts[offset(1)], NULL),
        customers.last_name)                                    as last_name,
    IFNULL(NULLIF(user_profiles.email, ''), customers.email)    as email,
    customers.registration_date,
    IFNULL(NULLIF(user_profiles.state, ''), customers.state)    as state,
    user_profiles.birth_date                                    as birth_date,
    gold.age_calculation(
        v_dag_logical_date,
        user_profiles.birth_date)                               as age,
    user_profiles.phone_number                                  as phone_number,

    customers._id,
    v_dag_logical_date,
    v_dag_start_date
FROM `{{ params.project_id }}.silver.customers` customers
LEFT JOIN `{{ params.project_id }}.silver.user_profiles` user_profiles
    ON customers.email = user_profiles.email
;