import time
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr
from SQL_medallion.lib.spark_session import get_session

APP_NAME = "Bronze_Layer_Load_csv"
JDBC_URL = "jdbc:postgresql://pgdatabase:5432/Medillion"
JDBC_USER = "root"
JDBC_PASSWORD = "root"
JDBC_DRIVER = "org.postgresql.Driver"

TABLE_CONFIGS = [
    {
        "layer_group": "CRM",
        "name": "crm_cust_info",
        "path": "s3a://sqlmedallion/pyspark_dw/landing/crm/cust_info.csv",
        "target_table": "bronze.crm_cust_info",
        "casts": {
            "cst_id": "cast(cst_id as int)",
            "cst_create_date": "to_date(cst_create_date,'yyyy-MM-dd')"
        }
    },
    {
        "layer_group": "CRM",
        "name": "crm_prd_info",
        "path": "s3a://sqlmedallion/pyspark_dw/landing/crm/prd_info.csv",
        "target_table": "bronze.crm_prd_info",
        "casts": {
            "prd_id": "cast(prd_id as int)",
            "prd_cost": "cast(prd_cost as int)",
            "prd_start_dt": "to_timestamp(prd_start_dt,'yyyy-MM-dd')",
            "prd_end_dt": "to_timestamp(prd_end_dt,'yyyy-MM-dd')"
        }
    },
    {
        "layer_group": "CRM",
        "name": "crm_sales_details",
        "path": "s3a://sqlmedallion/pyspark_dw/landing/crm/sales_details.csv",
        "target_table": "bronze.crm_sales_details",
        "casts": {
            "sls_cust_id": "cast(sls_cust_id as int)",
            "sls_order_dt": "cast(sls_order_dt as int)",
            "sls_ship_dt": "cast(sls_ship_dt as int)",
            "sls_due_dt": "cast(sls_due_dt as int)",
            "sls_sales": "cast(sls_sales as int)",
            "sls_quantity": "cast(sls_quantity as int)",
            "sls_price": "cast(sls_price as int)"
        }
    },
    {
        "layer_group": "ERP",
        "name": "erp_loc_a101",
        "path": "s3a://sqlmedallion/pyspark_dw/landing/erp/LOC_A101.csv",
        "target_table": "bronze.erp_loc_a101",
        "casts": {}
    },
    {
        "layer_group": "ERP",
        "name": "erp_cust_az12",
        "path": "s3a://sqlmedallion/pyspark_dw/landing/erp/CUST_AZ12.csv",
        "target_table": "bronze.erp_cust_az12",
        "casts": {
            "BDATE": "to_date(BDATE,'yyyy-MM-dd')"
        }
    },
    {
        "layer_group": "ERP",
        "name": "erp_px_cat_g1v2",
        "path": "s3a://sqlmedallion/pyspark_dw/landing/erp/PX_CAT_G1V2.csv",
        "target_table": "bronze.erp_px_cat_g1v2",
        "casts": {}
    }
]


def create_dw_databases(spark) -> None:
    spark.sql("CREATE DATABASE IF NOT EXISTS dw_gold")
    spark.sql("CREATE DATABASE IF NOT EXISTS dw_silver")
    spark.sql("CREATE DATABASE IF NOT EXISTS dw_bronze")


def apply_casts(df: DataFrame, casts: dict) -> DataFrame:
    for col_name, expression in casts.items():
        df = df.withColumn(col_name, expr(expression))
    return df


def truncate_postgres_table(spark, table_name: str) -> None:
    truncate_query = f"TRUNCATE TABLE {table_name}"

    (
        spark.read
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("query", truncate_query)
        .option("user", JDBC_USER)
        .option("password", JDBC_PASSWORD)
        .option("driver", JDBC_DRIVER)
        .load()
    )


def truncate_postgres_table_via_psycopg2(table_name: str) -> None:
    import psycopg2

    conn = psycopg2.connect(
        host="pgdatabase",
        port=5432,
        dbname="Medillion",
        user=JDBC_USER,
        password=JDBC_PASSWORD
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f"TRUNCATE TABLE {table_name}")
    cur.close()
    conn.close()


def load_one_table(spark, config: dict) -> None:
    start_time = time.time()

    print(f">> Truncating Table: {config['target_table']}")
    truncate_postgres_table_via_psycopg2(config["target_table"])

    print(f">> Inserting Data Into: {config['target_table']}")

    df = (
        spark.read
        .format("csv")
        .option("header", True)
        .load(config["path"])
    )

    df_fixed = apply_casts(df, config["casts"])

    row_count = df_fixed.count()

    (
        df_fixed.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", config["target_table"])
        .option("user", JDBC_USER)
        .option("password", JDBC_PASSWORD)
        .option("driver", JDBC_DRIVER)
        .mode("append")
        .save()
    )

    end_time = time.time()
    duration = round(end_time - start_time, 2)

    print(f">> Rows Loaded: {row_count}")
    print(f">> Load Duration: {duration} seconds")
    print(">> -------------")


def main() -> None:
    batch_start_time = time.time()
    spark = get_session(APP_NAME)

    try:
        print("================================================")
        print("Loading Bronze Layer")
        print("================================================")

        create_dw_databases(spark)
        spark.sql("SHOW DATABASES").show(truncate=False)

        crm_tables = [t for t in TABLE_CONFIGS if t["layer_group"] == "CRM"]
        erp_tables = [t for t in TABLE_CONFIGS if t["layer_group"] == "ERP"]

        print("------------------------------------------------")
        print("Loading CRM Tables")
        print("------------------------------------------------")
        for table_config in crm_tables:
            load_one_table(spark, table_config)

        print("------------------------------------------------")
        print("Loading ERP Tables")
        print("------------------------------------------------")
        for table_config in erp_tables:
            load_one_table(spark, table_config)

        batch_end_time = time.time()
        total_duration = round(batch_end_time - batch_start_time, 2)

        print("==========================================")
        print("Loading Bronze Layer is Completed")
        print(f"   - Total Load Duration: {total_duration} seconds")
        print("==========================================")

    except Exception as e:
        print("==========================================")
        print("ERROR OCCURED DURING LOADING BRONZE LAYER")
        print(f"Error Message: {str(e)}")
        print("==========================================")
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()