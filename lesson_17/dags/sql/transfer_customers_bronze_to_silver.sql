DECLARE v_dag_logical_date DEFAULT CAST(CAST('{{ dag_run.logical_date }}' AS TIMESTAMP) AS DATE);
DECLARE v_dag_start_date DEFAULT CAST('{{ dag_run.start_date }}' AS TIMESTAMP);
DECLARE v_dag_ds DEFAULT CAST(CAST('{{ ds }}' AS TIMESTAMP) AS DATE);

MERGE INTO `{{ params.project_id }}.silver.customers` target
USING (SELECT CAST(Id as INT64)                     AS Id,
              TRIM(FirstName)                       AS FirstName,
              TRIM(LastName)                        AS LastName,
              TRIM(Email)                           AS Email,
              CAST(TRIM(RegistrationDate) AS DATE)  AS RegistrationDate,
              TRIM(State)                           AS State,

              _id,
              _logical_dt,
              _job_start_dt
       FROM `{{ params.project_id }}.bronze.customers`
) source ON source.Id = target.client_id
WHEN MATCHED THEN UPDATE SET
        first_name = source.FirstName,
        last_name = source.LastName,
        email = source.Email,
        registration_date = source.RegistrationDate,
        state = source.State,

        _id = source._id,
        _logical_dt = source._logical_dt,
        _job_start_dt = source._job_start_dt
WHEN NOT MATCHED THEN INSERT(
        client_id,
        first_name,
        last_name,
        email,
        registration_date,
        state,

        _id,
        _logical_dt,
        _job_start_dt
    ) VALUES (
        source.Id,
        source.FirstName,
        source.LastName,
        source.Email,
        source.RegistrationDate,
        source.State,

        source._id,
        source._logical_dt,
        source._job_start_dt
    )
;