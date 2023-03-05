MERGE INTO `{{ params.project_id }}.silver.customers` target
USING (SELECT CAST(Id as INT64) AS Id,
              FirstName,
              LastName,
              Email,
              CAST(RegistrationDate AS DATE) as RegistrationDate,
              State,
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