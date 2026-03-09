SELECT 
REPLACE(cid,'-','') AS cid,
CASE WHEN UPPER(TRIM(cntry)) IN ('DE') THEN 'Germany'
	 WHEN UPPER(TRIM(cntry)) IN ('US','USA') THEN 'United States'
	 WHEN UPPER(TRIM(cntry)) IS NULL OR TRIM(cntry) = '' THEN 'n/a'
	 ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101


--WHERE REPLACE(cid,'-','') NOT IN (
--SELECT cst_key FROM silver.crm_cust_info)

--SELECT DISTINCT(cntry) FROM bronze.erp_loc_a101


-- INSERT in SILVER
INSERT INTO silver.erp_loc_a101
(
 cid,
 cntry
)
SELECT 
REPLACE(cid,'-','') AS cid,
CASE WHEN UPPER(TRIM(cntry)) IN ('DE') THEN 'Germany'
	 WHEN UPPER(TRIM(cntry)) IN ('US','USA') THEN 'United States'
	 WHEN UPPER(TRIM(cntry)) IS NULL OR TRIM(cntry) = '' THEN 'n/a'
	 ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101