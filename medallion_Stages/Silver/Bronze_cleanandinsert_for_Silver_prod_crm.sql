-- FOR TABLE bronze.crm_cust_info

SELECT * FROM bronze.crm_prd_info

--Checking Null or Duplicates

SELECT prd_id,COUNT(*) FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*)>1 or prd_id IS NULL;

---- Extract a part of the product key and when the result comes replace the - with a _ 
SELECT prd_key,
REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id
FROM bronze.crm_prd_info

---- Check for cat_ids which are not matching the cat_id of erp_px_cat_g1v2
--SELECT * FROM bronze.erp_px_cat_g1v2

SELECT prd_key,
REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id
FROM bronze.crm_prd_info
WHERE REPLACE(SUBSTRING(prd_key,1,5),'-','_') NOT IN(SELECT id FROM bronze.erp_px_cat_g1v2)

-- We see 1 catgory: CO_PE not available

---- Extract the second part from the string

SELECT prd_key,
REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
SUBSTRING(prd_key,7,LENGTH(prd_key)) AS _prd_key
FROM bronze.crm_prd_info

--- Sales Details table with which we need to join the _prd_key
SELECT * FROM bronze.crm_sales_details;
--checking

SELECT prd_key,
REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
SUBSTRING(prd_key,7,LENGTH(prd_key)) AS _prd_key
FROM bronze.crm_prd_info
WHERE SUBSTRING(prd_key,7,LENGTH(prd_key)) 
NOT IN (SELECT sls_prd_key FROM bronze.crm_sales_details)\

-------- Checking if the prd_nm haves spaces or not : Zero results
SELECT * FROM bronze.crm_prd_info WHERE prd_nm != TRIM(prd_nm)

---------  checking prd_cost : NULL or NEGETIVE
SELECT * FROM bronze.crm_prd_info WHERE prd_cost < 0 OR prd_cost IS NULL

-- replace NULL with zero

SELECT COALESCE(prd_cost,0) FROM bronze.crm_prd_info WHERE prd_cost < 0 OR prd_cost IS NULL

-- checking prd_line

SELECT DISTINCT(prd_line) FROM bronze.crm_prd_info
-- we replace it with CASES

-- check prd_start_date and prd_end_date
SELECT * FROM bronze.crm_prd_info
WHERE prd_start_dt > prd_end_dt

--- Fixing this by we remove the end_date and then repopulate it by next row start date - 1
SELECT * FROM bronze.crm_prd_info WHERE prd_key='AC-HE-HL-U509-R';

SELECT prd_key,prd_start_dt,
LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 DAY' 
AS prd_end_date
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R','AC-HE-HL-U509')
--GROUP BY prd_key
--ORDER BY prd_key








--- With all replacement we have
INSERT INTO silver.crm_prd_info(
	prd_id ,
    cat_id ,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT 
	prd_id ,
    REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id ,
	SUBSTRING(prd_key,7,LENGTH(prd_key)) AS prd_key,
    prd_nm,
    COALESCE(prd_cost,0) AS prd_cost,
    CASE WHEN UPPER(TRIM(prd_line))='M' THEN 'Mountain'
		 WHEN UPPER(TRIM(prd_line))='R' THEN 'Road'
		 WHEN UPPER(TRIM(prd_line))='S' THEN 'Other Sales'
		 WHEN UPPER(TRIM(prd_line))='T' THEN 'Touring'
		 ELSE 'n/a'
    END AS prd_line,
    CAST(prd_start_dt AS DATE) AS prd_start_dt ,
    CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 DAY' 
	AS DATE) AS prd_end_date
FROM bronze.crm_prd_info

----
SELECT * FROM silver.crm_prd_info
