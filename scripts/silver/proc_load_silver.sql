
call silver.load_silver();

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE NOTICE 'Truncating table: silver.crm_cust_info';
	truncate table silver.crm_cust_info;
	RAISE NOTICE 'Inserting Data into: silver.crm_cust_info';

	insert into silver.crm_prd_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)

	SELECT 
	    prd_id, 
	    REPLACE(SUBSTR(prd_key, 1, 5), '-', '_') AS cat_id,
	    SUBSTR(prd_key, 7) as prd_key, 
	    prd_nm, 
	    COALESCE(prd_cost, 0) as prd_cost, 
	    case UPPER(TRIM(prd_line))
	    	 when 'M' then 'Mountain'
	    	 when 'R' then 'Road'
	    	 when 'S' then 'Other Sales'
	    	 when 'T' then 'Touring'
	    	 else 'n/a'
	    end as prd_line,
	    cast(prd_start_dt as DATE) as prd_start_dt, 
	    cast(LEAD(prd_start_dt) over (partition by prd_key order by prd_start_dt) - INTERVAL '1 day' as DATE) as prd_end_dt
	FROM 
	    bronze.crm_prd_info;
    
	RAISE NOTICE 'Truncating table: silver.crm_sales_details';
	truncate table silver.crm_sales_details;
	RAISE NOTICE 'Inserting Data into: silver.crm_sales_details';

	insert into silver.crm_sales_details (
		sls_ord_num ,
	    sls_prd_key ,
	    sls_cust_id  ,
	    sls_order_dt ,
	    sls_ship_dt  ,
	    sls_due_dt   ,
	    sls_sales    ,
	    sls_quantity ,
	    sls_price   
    )

	SELECT 
		sls_ord_num, 
		sls_prd_key, 
		sls_cust_id, 
		case when sls_order_dt  = 0 or length(sls_order_dt::TEXT) != 8 then null 
			 else cast(cast(sls_order_dt as varchar) as date)
		end as sls_order_dt, 
		case when sls_ship_dt  = 0 or length(sls_ship_dt::TEXT) != 8 then null 
			 else cast(cast(sls_ship_dt as varchar) as date)
		end as sls_ship_dt, 
		case when sls_due_dt  = 0 or length(sls_due_dt::TEXT) != 8 then null 
			 else cast(cast(sls_due_dt as varchar) as date)
		end as sls_due_dt, 
			case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price)
				then sls_quantity * abs(sls_price)
			 else sls_sales
		end as sls_sales,
		sls_quantity,
		case when sls_price is null or sls_price <=0
				then sls_sales / nullif(sls_quantity, 0)
			else sls_price
		end
		
	FROM bronze.crm_sales_details;

	RAISE NOTICE 'Truncating table: silver.crm_sales_details';
	truncate table silver.crm_sales_details;
	RAISE NOTICE 'Inserting Data into: silver.crm_sales_details';

	insert into silver.crm_sales_details (
		sls_ord_num ,
	    sls_prd_key ,
	    sls_cust_id  ,
	    sls_order_dt ,
	    sls_ship_dt  ,
	    sls_due_dt   ,
	    sls_sales    ,
	    sls_quantity ,
	    sls_price   
	    )
	
	SELECT 
		sls_ord_num, 
		sls_prd_key, 
		sls_cust_id, 
		case when sls_order_dt  = 0 or length(sls_order_dt::TEXT) != 8 then null 
			 else cast(cast(sls_order_dt as varchar) as date)
		end as sls_order_dt, 
		case when sls_ship_dt  = 0 or length(sls_ship_dt::TEXT) != 8 then null 
			 else cast(cast(sls_ship_dt as varchar) as date)
		end as sls_ship_dt, 
		case when sls_due_dt  = 0 or length(sls_due_dt::TEXT) != 8 then null 
			 else cast(cast(sls_due_dt as varchar) as date)
		end as sls_due_dt, 
			case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price)
				then sls_quantity * abs(sls_price)
			 else sls_sales
		end as sls_sales,
		sls_quantity,
		case when sls_price is null or sls_price <=0
				then sls_sales / nullif(sls_quantity, 0)
			else sls_price
		end
		
	FROM bronze.crm_sales_details;

	RAISE NOTICE 'Truncating table: silver.erp_cust_az12';
	truncate table silver.erp_cust_az12;
	RAISE NOTICE 'Inserting Data into: silver.erp_cust_az12';

	insert into silver.erp_cust_az12 (
		cid, bdate, gen
	)
	
	select 
		case when cid like 'NAS%' then substr(cid, 4, length(cid))
			 else cid
		end as cid,
		case when bdate > now() then null 
			 else bdate
		end as bdate,
		case when UPPER(TRIM(gen)) in ('F', 'Female') then 'Female'
			 when UPPER(TRIM(gen)) in ('M', 'Male') then 'Male'
			 else 'n/a'
		end as gen
	from bronze.erp_cust_az12; 	

	RAISE NOTICE 'Truncating table: silver.erp_px_cat_g1v2';
	truncate table silver.erp_cust_az12;
	RAISE NOTICE 'Inserting Data into: silver.erp_px_cat_g1v2';

	insert into silver.erp_px_cat_g1v2 (
	    id,
	    cat,
	    subcat,
	    maintenance
	)
	
	select  id,
	    cat,
	    subcat,
	    maintenance
	from bronze.erp_px_cat_g1v2;

	RAISE NOTICE 'Truncating table: silver.erp_loc_a101';
	truncate table silver.erp_loc_a101;
	RAISE NOTICE 'Inserting Data into: silver.erp_loc_a101';

	insert into silver.erp_loc_a101 (cid, cntry)

	select replace(cid, '-', '') cid, 
		case when TRIM(cntry) = 'DE' then 'Germany'
			 when TRIM(cntry) in ('US', 'USA') then 'United States'
			 when TRIM(cntry) = '' or cntry is null then 'n/a'
			 else TRIM(cntry)
	end as cntry
	from bronze.erp_loc_a101;

    -- Unlike functions, procedures can manage transactions
    COMMIT; 
END;
$$;

	

