-- FOR TABLE bronze.crm_cust_info

SELECT * FROM bronze.crm_cust_info

--Checking Null or Duplicates

SELECT cst_id,COUNT(*) FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*)>1 or cst_id IS NULL;

-- Correcting the Null and Duplicates

-- First checking the duplicates.
SELECT * FROM bronze.crm_cust_info where cst_id =29466;

-- We see we need to take the latest info i.e. cst_create_date needs to be latest
-- also putting IS NOT NULL in the nested subquery to remove the NULLs

SELECT * FROM 
(
SELECT *,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL) AS DE_DUPE
WHERE DE_DUPE.rn=1


--Check for unwanted Spaces
SELECT
cst_id,
cst_key,
TRIM(cst_firstname) as cst_firstname,
TRIM(cst_lastname) as cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date
FROM (SELECT *,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL) AS DE_DUPE
WHERE DE_DUPE.rn=1

---------- CUST_KEY CHECK : results zero.. so Its good
SELECT * FROM bronze.crm_cust_info
WHERE cst_key!=TRIM(cst_key)

---------- marital status and gender check : low cardinality values
SELECT DISTINCT(cst_marital_status) FROM bronze.crm_cust_info
SELECT DISTINCT(cst_gndr) FROM bronze.crm_cust_info


--- updating marital status and gender check
SELECT
cst_id,
cst_key,
TRIM(cst_firstname) as cst_firstname,
TRIM(cst_lastname) as cst_lastname,
CASE WHEN UPPER(TRIM(cst_gndr))='F' THEN 'Female'
	 WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
	 ELSE 'n/a'
END,
CASE WHEN UPPER(TRIM(cst_marital_status))='S' THEN 'Single'
	 WHEN UPPER(TRIM(cst_marital_status))='M' THEN 'Married'
	 ELSE 'n/a'
END,
cst_create_date
FROM (SELECT *,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL) AS DE_DUPE
WHERE DE_DUPE.rn=1

---- Inserting it in to silver.cust_info

--SELECT * FROM silver.crm_cust_info

INSERT INTO silver.crm_cust_info (
	cst_id ,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
SELECT
cst_id,
cst_key,
TRIM(cst_firstname) as cst_firstname,
TRIM(cst_lastname) as cst_lastname,
CASE WHEN UPPER(TRIM(cst_gndr))='F' THEN 'Female'
	 WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
	 ELSE 'n/a'
END,
CASE WHEN UPPER(TRIM(cst_marital_status))='S' THEN 'Single'
	 WHEN UPPER(TRIM(cst_marital_status))='M' THEN 'Married'
	 ELSE 'n/a'
END,
cst_create_date
FROM (SELECT *,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn
FROM bronze.crm_cust_info
WHERE cst_id IS NOT NULL) AS DE_DUPE
WHERE DE_DUPE.rn=1

--- VALIDATION CHECKS ---

SELECT cst_id,COUNT(*) FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*)>1 or cst_id IS NULL;

---------- CUST_KEY CHECK : results zero.. so Its good

SELECT * FROM silver.crm_cust_info
WHERE cst_firstname!=TRIM(cst_firstname)

SELECT * FROM silver.crm_cust_info
WHERE cst_lastname!=TRIM(cst_lastname)

SELECT * FROM silver.crm_cust_info
WHERE cst_key!=TRIM(cst_key)

---------- marital status and gender check : low cardinality values
SELECT DISTINCT(cst_marital_status) FROM silver.crm_cust_info;
SELECT DISTINCT(cst_gndr) FROM silver.crm_cust_info;

------
SELECT * FROM silver.crm_cust_info