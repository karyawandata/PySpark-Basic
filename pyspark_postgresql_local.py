from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from config.config_postgres_local_bit import POSTGRES_CONFIG

# Create a SparkSession with PostgreSQL driver
spark = SparkSession.builder \
    .appName("PostgreSQLLocalBIT") \
    .config("spark.driver.extraClassPath", "C:\\SparkProject\\jars\\postgresql-42.2.18.jar") \
    .config("spark.executor.extraClassPath", "C:\\SparkProject\\jars\\postgresql-42.2.18.jar") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.allowMultipleContexts", "true") \
    .config("spark.local.dir", "C:/SparkTemp") \
    .master("local[*]") \
    .getOrCreate()

# Define the PostgreSQL database connection details
jdbc_url = f"jdbc:postgresql://{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['dbname']}"
connection_properties = {
    "user": POSTGRES_CONFIG["user"],
    "password": POSTGRES_CONFIG["password"],
    "driver": "org.postgresql.Driver"
}

# Read data from PostgreSQL using Spark SQL
users_df = spark.read \
    .jdbc(url=jdbc_url,
          table='"INTransDtl"',
          properties=connection_properties)

# Query to select specific columns, filter for branchID > 1, and order by branchID
result = users_df.select("SiteID", "Sys", "LineNo","Qt","NetAmt")

print("Query result:")
result.show(truncate=False)

# Close the SparkSession
spark.stop()
