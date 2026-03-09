SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LENGTH(cid))
	 ELSE cid
END AS cid,
CASE WHEN bdate > NOW() THEN NULL
	 ELSE bdate
END AS bdate,
CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
	 ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12


-- check bdate
SELECT * FROM bronze.erp_cust_az12
WHERE bdate < '1927-01-01' or bdate > NOW()

--check gender
SELECT DISTINCT(gen) FROM bronze.erp_cust_az12


--- Insert in silver layer
INSERT INTO silver.erp_cust_az12
(
	cid,
    bdate,
    gen
)
SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LENGTH(cid))
	 ELSE cid
END AS cid,
CASE WHEN bdate > NOW() THEN NULL
	 ELSE bdate
END AS bdate,
CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
	 ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12

SELECT * FROM silver.erp_cust_az12