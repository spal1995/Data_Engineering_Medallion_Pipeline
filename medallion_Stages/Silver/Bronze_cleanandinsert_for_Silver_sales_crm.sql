SELECT 
	sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE WHEN sls_order_dt=0 OR LENGTH(CAST(sls_order_dt AS VARCHAR))!=8 THEN NULL
	 	 ELSE TO_DATE(CAST (sls_order_dt AS VARCHAR) , 'yyyyMMdd')
	END AS sls_order_dt,
	CASE WHEN sls_ship_dt=0 OR LENGTH(CAST(sls_ship_dt AS VARCHAR))!=8 THEN NULL
	 	 ELSE TO_DATE(CAST (sls_ship_dt AS VARCHAR) , 'yyyyMMdd')
	END AS sls_ship_dt,
	CASE WHEN sls_due_dt=0 OR LENGTH(CAST(sls_due_dt AS VARCHAR))!=8 THEN NULL
	 	 ELSE TO_DATE(CAST (sls_due_dt AS VARCHAR) , 'yyyyMMdd')
	END AS sls_due_dt,
    CASE WHEN sls_sales IS NULL OR sls_sales<=0 OR sls_sales!= sls_quantity*ABS(sls_price)
		THEN sls_quantity*ABS(sls_price)
		 ELSE sls_sales
	END AS sls_sales,
    sls_quantity,
    	CASE WHEN sls_price IS NULL OR sls_price<=0 THEN sls_sales/NULLIF(sls_quantity,0)
		 ELSE sls_price
	END AS sls_price
FROM bronze.crm_sales_details


-- Checks we did : Zero Results
--WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)
--WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)

-- Date Columns need to be changed from integer to Date
SELECT sls_order_dt,
TO_DATE(CAST(NULLIF(sls_order_dt,0) AS VARCHAR),'yyyyMMdd')
--TO_DATE(sls_order_dt,'yyyy-MM-dd')
FROM bronze.crm_sales_details WHERE LENGTH(CAST(sls_order_dt AS VARCHAR))=8
--WHERE sls_order_dt <= 0 or sls_order_dt>20500101

SELECT sls_order_dt
--CAST(CAST(NULLIF(sls_order_dt,0) AS VARCHAR) AS TIMESTAMP)
--TO_DATE(sls_order_dt,'yyyy-MM-dd')
FROM bronze.crm_sales_details where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt
--WHERE sls_order_dt <= 0 or sls_order_dt>20500101

-- Check Sales, Quantity and Price

SELECT 
	sls_sales,
    sls_quantity,
    sls_price,
	CASE WHEN sls_sales IS NULL OR sls_sales<=0 OR sls_sales!= sls_quantity*ABS(sls_price)
		THEN sls_quantity*ABS(sls_price)
		 ELSE sls_sales
	END AS sls_sales_,
	CASE WHEN sls_price IS NULL OR sls_price<=0 THEN sls_sales/NULLIF(sls_quantity,0)
		 ELSE sls_price
	END AS sls_price_
FROM bronze.crm_sales_details
WHERE sls_sales!=sls_quantity*sls_price
OR sls_sales<=0 OR sls_quantity<=0 OR sls_price<=0
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
ORDER BY sls_sales, sls_quantity, sls_price


----- INSERT INTO SALES SILVER TABLE

INSERT INTO silver.crm_sales_details (
	sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
SELECT 
	sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE WHEN sls_order_dt=0 OR LENGTH(CAST(sls_order_dt AS VARCHAR))!=8 THEN NULL
	 	 ELSE TO_DATE(CAST (sls_order_dt AS VARCHAR) , 'yyyyMMdd')
	END AS sls_order_dt,
	CASE WHEN sls_ship_dt=0 OR LENGTH(CAST(sls_ship_dt AS VARCHAR))!=8 THEN NULL
	 	 ELSE TO_DATE(CAST (sls_ship_dt AS VARCHAR) , 'yyyyMMdd')
	END AS sls_ship_dt,
	CASE WHEN sls_due_dt=0 OR LENGTH(CAST(sls_due_dt AS VARCHAR))!=8 THEN NULL
	 	 ELSE TO_DATE(CAST (sls_due_dt AS VARCHAR) , 'yyyyMMdd')
	END AS sls_due_dt,
    CASE WHEN sls_sales IS NULL OR sls_sales<=0 OR sls_sales!= sls_quantity*ABS(sls_price)
		THEN sls_quantity*ABS(sls_price)
		 ELSE sls_sales
	END AS sls_sales,
    sls_quantity,
    	CASE WHEN sls_price IS NULL OR sls_price<=0 THEN sls_sales/NULLIF(sls_quantity,0)
		 ELSE sls_price
	END AS sls_price
FROM bronze.crm_sales_details

---
SELECT * from silver.crm_sales_details