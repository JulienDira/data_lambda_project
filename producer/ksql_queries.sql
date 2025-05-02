CREATE STREAM transactions_stream (
 transaction_id STRING, 
 amount DOUBLE, 
 transaction_type STRING
) WITH (
 KAFKA_TOPIC='transaction_log', 
 VALUE_FORMAT='JSON'
);

CREATE STREAM transactions_importantes_700 AS 
SELECT * FROM transactions_stream WHERE amount > 700;

CREATE TABLE total_par_transaction_type AS 
SELECT transaction_type, SUM(amount) AS total_amount 
FROM transactions_stream 
GROUP BY transaction_type;

CREATE STREAM all_transactions (
  transaction_id VARCHAR,
  timestamp VARCHAR,
  user_id VARCHAR,
  user_name VARCHAR,
  product_id VARCHAR,
  amount DOUBLE,
  currency VARCHAR,
  transaction_type VARCHAR,
  status VARCHAR,
  location STRUCT<city VARCHAR, country VARCHAR>,
  payment_method VARCHAR,
  product_category VARCHAR,
  quantity INT,
  shipping_address STRUCT<street VARCHAR, zip VARCHAR, city VARCHAR, country 
VARCHAR>,
  device_info STRUCT<os VARCHAR, browser VARCHAR, ip_address VARCHAR>,
  customer_rating INT,
  discount_code VARCHAR,
  tax_amount DOUBLE,
  thread INT,
  message_number INT,
  timestamp_of_reception_log VARCHAR
 ) WITH (
  KAFKA_TOPIC='transaction_log', 
  VALUE_FORMAT='JSON'
 );

create table total_transaction_amount_per_payment_method as 
SELECT payment_method, SUM(amount) AS total_amount
 FROM all_transactions
 GROUP BY payment_method;

create table count_numb_buy_per_product as
 SELECT product_id, SUM(quantity) AS total_quantity
 FROM all_transactions
 GROUP BY product_id;

CREATE STREAM all_transactions_flattened AS
 SELECT 
  transaction_id,
  timestamp,
  user_id,
  user_name,
  product_id,
  amount,
  currency,
  transaction_type,
  status,
  location->city AS city,
  location->country AS country,
  payment_method,
  product_category,
  quantity,
  shipping_address->street AS shipping_street,
  shipping_address->zip AS shipping_zip,
  shipping_address->city AS shipping_city,
  shipping_address->country AS shipping_country,
  device_info->os AS device_os,
  device_info->browser AS device_browser,
  device_info->ip_address AS device_ip,
  customer_rating,
  discount_code,
  tax_amount,
  thread,
  message_number,
  timestamp_of_reception_log
 FROM all_transactions;

 CREATE STREAM all_transactions_anonymized AS
 SELECT 
  transaction_id,
  timestamp,
  MD5(user_id) AS user_id_hashed,
  MD5(user_name) AS user_name_hashed,
  product_id,
  amount,
  currency,
  transaction_type,
  status,
  city,
  country,
  payment_method,
  product_category,
  quantity,
  shipping_street,
  shipping_zip,
  shipping_city,
  shipping_country,
  device_os,
  device_browser,
  CONCAT(SUBSTRING(device_ip, 1, 8), 'xxx.xxx') AS masked_ip,
  customer_rating,
  discount_code,
  tax_amount,
  thread,
  message_number,
  timestamp_of_reception_log
 FROM all_transactions_flattened;

 CREATE STREAM transactions_converted AS
 SELECT 
  *,
  CASE 
    WHEN currency = 'USD' THEN amount
    WHEN currency = 'EUR' THEN amount * 1.08
    WHEN currency = 'GBP' THEN amount * 1.24
    WHEN currency = 'CAD' THEN amount * 0.75
    WHEN currency = 'JPY' THEN amount * 0.007
    WHEN currency = 'AUD' THEN amount * 0.66
    ELSE NULL
  END AS amount_usd
 FROM all_transactions_anonymized;

CREATE STREAM transactions_blacklist AS
 SELECT *
 FROM transactions_converted
 WHERE 
  (city = 'Glasgow' AND country = 'UK') OR
  (city = 'Shenzhen' AND country = 'China') OR
  (city = 'Berlin' AND country = 'Germany');

 CREATE STREAM transactions_converted_cleaned AS
 SELECT *
 FROM transactions_converted
 WHERE NOT (
  (city = 'Glasgow' AND country = 'UK') OR
  (city = 'Shenzhen' AND country = 'China') OR
  (city = 'Berlin' AND country = 'Germany')
 );


 CREATE TABLE total_spent_per_user_transaction_type AS
 SELECT concat(user_id_hashed, '-',transaction_type), SUM(amount_usd) AS total_spent
 FROM TRANSACTIONS_CONVERTED_CLEANED
 GROUP BY concat(user_id_hashed, '-', transaction_type);

 CREATE STREAM transactions_completed AS
 SELECT *
 FROM TRANSACTIONS_CONVERTED_CLEANED
 WHERE status = 'completed';

 CREATE STREAM transactions_pending AS
 SELECT *
 FROM TRANSACTIONS_CONVERTED_CLEANED
 WHERE status = 'pending';

 CREATE STREAM transactions_failed AS
 SELECT *
 FROM TRANSACTIONS_CONVERTED_CLEANED
 WHERE status = 'failed';

 CREATE STREAM transactions_processing AS
 SELECT *
 FROM TRANSACTIONS_CONVERTED_CLEANED
 WHERE status = 'processing';

 CREATE STREAM transactions_cancelled AS
 SELECT *
 FROM TRANSACTIONS_CONVERTED_CLEANED
 WHERE status = 'cancelled';

CREATE STREAM transaction_log_stream (
  transaction_id VARCHAR,
  latest_status VARCHAR
) WITH (
  KAFKA_TOPIC='transaction_log',
  VALUE_FORMAT='JSON'
);

CREATE TABLE transaction_status_evolution AS
SELECT
  transaction_id,
  LATEST_BY_OFFSET(latest_status) AS latest_status
FROM transaction_log_stream
GROUP BY transaction_id;

 CREATE TABLE transaction_status_evolution (
  transaction_id VARCHAR PRIMARY KEY,
  latest_status VARCHAR
 ) WITH (KAFKA_TOPIC='transaction_log', VALUE_FORMAT='JSON');

CREATE TABLE amount_per_type_windowed AS
 SELECT 
  transaction_type,
  WINDOWSTART AS window_start,
  WINDOWEND AS window_end,
  SUM(amount_usd) AS total_amount
 FROM TRANSACTIONS_CONVERTED_CLEANED
 WINDOW HOPPING (SIZE 5 MINUTES, ADVANCE BY 1 MINUTE)
 GROUP BY transaction_type;