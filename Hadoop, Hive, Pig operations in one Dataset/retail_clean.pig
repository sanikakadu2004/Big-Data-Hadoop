-- Load raw CSV data
raw_data = LOAD '/user/sanika/sales_project/raw_data/retail_sales_dataset.csv'
USING PigStorage(',')
AS (transaction_id:int, date:chararray, customer_id:int, gender:chararray, age:int,
    product_category:chararray, quantity:int, price_per_unit:double, total_amount:double);

-- Remove header (if any)
no_header = FILTER raw_data BY transaction_id != 0;

-- Remove invalid or missing data
cleaned_data = FILTER no_header BY (transaction_id IS NOT NULL) AND (total_amount > 0);

-- Store cleaned data into new HDFS location
STORE cleaned_data INTO '/user/sanika/sales_project/cleaning_data'
USING PigStorage(',');
