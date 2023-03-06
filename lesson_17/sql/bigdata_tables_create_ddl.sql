Accidental run-all protection

CREATE OR REPLACE TABLE `de-07-ageiev-oleksii-l17.bronze.sales`
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

CREATE OR REPLACE TABLE `de-07-ageiev-oleksii-l17.bronze.customers`
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


CREATE OR REPLACE TABLE `de-07-ageiev-oleksii-l17.silver.sales`
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

CREATE OR REPLACE TABLE `de-07-ageiev-oleksii-l17.silver.customers`
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


CREATE OR REPLACE TABLE `de-07-ageiev-oleksii-l17.bronze.user_profiles`
(
  email STRING,
  full_name STRING,
  state STRING,
  birth_date STRING,
  phone_number STRING,

  _id STRING(36) NOT NULL,
  _logical_dt DATE NOT NULL,
  _job_start_dt TIMESTAMP NOT NULL
)
;

CREATE OR REPLACE TABLE `de-07-ageiev-oleksii-l17.silver.user_profiles`
(
  email STRING,
  full_name STRING,
  state STRING,
  birth_date DATE,
  phone_number STRING,
  
  full_name_parts ARRAY<STRING>,
  
  _id STRING(36) NOT NULL,
  _logical_dt DATE NOT NULL,
  _job_start_dt TIMESTAMP NOT NULL
)
;

CREATE OR REPLACE TABLE `de-07-ageiev-oleksii-l17.gold.user_profiles_enriched`
(
  client_id INT64,
  first_name STRING,
  last_name STRING,
  email STRING,
  registration_date DATE,
  state STRING,
  birth_date DATE,
  age int,
  phone_number STRING,
  
  _id STRING(36) NOT NULL,
  _logical_dt DATE NOT NULL,
  _job_start_dt TIMESTAMP NOT NULL
)
;

CREATE OR REPLACE FUNCTION `de-07-ageiev-oleksii-l17.gold.age_calculation`(
	as_of_date DATE, 
	date_of_birth DATE
) AS (
	  DATE_DIFF(as_of_date, date_of_birth, YEAR)
  -	IF(   EXTRACT(MONTH FROM date_of_birth)*100 + EXTRACT(DAY FROM date_of_birth)
        > EXTRACT(MONTH FROM as_of_date)*100 + EXTRACT(DAY FROM as_of_date)
        ,1,0)
)
;
