CREATE TABLE `de-07-ageiev-oleksii-l17.bronze.sales`
(
  CustomerId STRING,
  PurchaseDate STRING,
  Product STRING,
  Price STRING,
  _id STRING(36) NOT NULL,
  _logical_dt DATE NOT NULL,
  _job_start_dt TIMESTAMP NOT NULL
)
PARTITION BY (_logical_dt, DAY)
OPTIONS(
  require_partition_filter = TRUE
)
;

CREATE TABLE `de-07-ageiev-oleksii-l17.bronze.customers`
(
  Id STRING,
  FirstName STRING,
  LastName STRING,
  Email STRING,
  RegistrationDate STRING,
  State STRING,
  _id STRING(36) NOT NULL,
  _logical_dt DATE NOT NULL,
  _job_start_dt TIMESTAMP NOT NULL
)
;


CREATE TABLE `de-07-ageiev-oleksii-l17.silver.sales`
(
  client_id INT64,
  purchase_date DATE,
  product STRING,
  price NUMERIC,
  currency STRING(5),
  _id STRING(36) NOT NULL,
  _logical_dt DATE NOT NULL,
  _job_start_dt TIMESTAMP NOT NULL
)
PARTITION BY purchase_date
OPTIONS(
  require_partition_filter = TRUE
)
;

CREATE TABLE `de-07-ageiev-oleksii-l17.silver.customers`
(
  client_id INT64,
  first_name STRING,
  last_name STRING,
  email STRING,
  registration_date DATE,
  state STRING,
  _id STRING(36) NOT NULL,
  _logical_dt DATE NOT NULL,
  _job_start_dt TIMESTAMP NOT NULL
)
;


