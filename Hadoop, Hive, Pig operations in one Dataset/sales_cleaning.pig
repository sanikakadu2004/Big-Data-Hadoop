-- Load data
sales = LOAD '/user/sanika/sales_project/sales_data.csv'
USING PigStorage(',')
AS (OrderID:int, Date:chararray, Region:chararray, Product:chararray, Category:chararray, Sales:int);

-- Remove header
no_header = FILTER sales BY OrderID != 1;

-- Remove null or invalid entries (if any)
clean_data = FILTER no_header BY (Region is not null) AND (Sales > 0);

-- Store cleaned data
STORE clean_data INTO '/user/sanika/sales_project/cleaned_data' USING PigStorage(',');
