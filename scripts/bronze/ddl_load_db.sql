docker cp "C:\Users\SSLTP11713\dataware-house-project\dataset\source_crm\cust_info.csv" postgres_local:/tmp/cust_info.csv

COPY bronze.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
FROM '/tmp/cust_info.csv'
DELIMITER ','
CSV HEADER;

docker cp "C:\Users\SSLTP11713\dataware-house-project\dataset\source_crm\prd_info.csv" postgres_local:/tmp/prd_info.csv

copy bronze.crm_prd_info (prd_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)
from '/tmp/prd_info.csv'
delimiter ','
csv header;

docker cp "C:\Users\SSLTP11713\dataware-house-project\dataset\source_crm\sales_details.csv" postgres_local:/tmp/sales_details.csv

copy bronze.crm_sales_details (sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price)
from '/tmp/sales_details.csv'
delimiter ','
csv header;

docker cp "C:\Users\SSLTP11713\dataware-house-project\dataset\source_erp\CUST_AZ12.csv" postgres_local:/tmp/CUST_AZ12.csv

copy bronze.erp_cust_az12 (cid,bdate,gen)
from '/tmp/CUST_AZ12.csv'
delimiter ','
csv header;

docker cp "C:\Users\SSLTP11713\dataware-house-project\dataset\source_erp\LOC_A101.csv" postgres_local:/tmp/LOC_A101.csv

copy bronze.erp_loc_a101 (cid,cntry)
from '/tmp/LOC_A101.csv'
delimiter ','
csv header;

docker cp "C:\Users\SSLTP11713\dataware-house-project\dataset\source_erp\PX_CAT_G1V2.csv" postgres_local:/tmp/PX_CAT_G1V2.csv

copy bronze.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
from '/tmp/PX_CAT_G1V2.csv'
delimiter ','
csv header;
