Accidental run-all protection

-- external table tests
CREATE EXTERNAL TABLE bronze.2022_08_1_part1__customers_csv
  (
    Id STRING,
    FirstName STRING,
    LastName STRING,
    Email STRING,
    RegistrationDate STRING,
    State STRING,
  )
  OPTIONS(
    format='CSV',
    skip_leading_rows=1,
    field_delimiter=',',
    uris = ['gs://de-07-bucket-aoleksii-l17/customers/2022-08-1/2022-08-1_part1__customers.csv']
  )


select * from bronze.2022_08_1_part1__customers_csv
limit 100


CREATE EXTERNAL TABLE bronze.2022_09_1__sales_csv
  (
    CustomerId STRING,
    PurchaseDate STRING,
    Product STRING,
    Price STRING,
  )
  OPTIONS(
    format='CSV',
    skip_leading_rows=1,
    field_delimiter=',',
    uris = ['gs://de-07-bucket-aoleksii-l17/sales/2022-09-1/2022-09-1__sales.csv']
  )

select * from bronze.2022_09_1__sales_csv
limit 100


-- partitions check

select * from `de-07-ageiev-oleksii-l17.bronze.INFORMATION_SCHEMA.PARTITIONS`
limit 100

-- check sales
select * from `de-07-ageiev-oleksii-l17.bronze.sales`
where `_logical_dt` = cast('2022-09-01' as DATE)
limit 100


