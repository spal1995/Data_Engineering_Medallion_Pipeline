SELECT
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2

--SELECT * FROM silver.crm_prd_info

--check unwanted spaces
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat!=TRIM(cat)

SELECT * FROM bronze.erp_px_cat_g1v2
WHERE subcat!=TRIM(subcat)

SELECT * FROM bronze.erp_px_cat_g1v2
WHERE maintenance!=TRIM(maintenance)

-- Data Standadization
SELECT DISTINCT maintenance FROM bronze.erp_px_cat_g1v2


---INSERT in Silver
INSERT INTO silver.erp_px_cat_g1v2
(
	id,
    cat,
    subcat,
    maintenance
)
SELECT
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2


SELECT * FROM silver.erp_px_cat_g1v2