# Databricks notebook source
print("fdjh")

# COMMAND ----------

print("poc")

# COMMAND ----------

print("hii")

# COMMAND ----------

print("hello")

# COMMAND ----------

# DBTITLE 1,ADLS Gen2 Access Config
# Direct ADLS Gen2 Access using Storage Account Access Key
# No mount needed — works on all cluster types including Shared (USER_ISOLATION)

# ---- Configuration ----
storage_account_name = "staffbaseadlspoc"
container_name = "raw"

# Using Databricks Secret Scope (secure — no hardcoded keys)
access_key = dbutils.secrets.get(scope="adls-poc", key="storage-account-key")

# ---- Set Access Key in Spark Config ----
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    access_key
)

print(f"Access configured for storage account: {storage_account_name}")
print(f"You can now read/write using: abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/")

# COMMAND ----------

# Verify access by listing files in the ADLS container
adls_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"

display(dbutils.fs.ls(adls_path))

# COMMAND ----------

from pyspark.sql.functions import rand, round, concat, lit, col

# ============================================
# Step 2: Create a Random DataFrame
# ============================================
df = spark.range(100).select(
    col("id").alias("employee_id"),
    concat(lit("emp_"), col("id")).alias("name"),
    (rand() * 50 + 20).cast("int").alias("age"),
    round(rand() * 100000 + 30000, 2).alias("salary")
)

print("Random DataFrame created with 100 rows:")
display(df)

# COMMAND ----------

# ============================================
# Step 3: Write DataFrame as JSON to ADLS via direct ABFSS path
# ============================================
adls_base_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net"
output_path = f"{adls_base_path}/poc_output/employee_data_json"

df.write.format("json").mode("overwrite").save(output_path)

print(f"Successfully written JSON to: {output_path}")

# Verify files were written
display(dbutils.fs.ls(output_path))

# COMMAND ----------


