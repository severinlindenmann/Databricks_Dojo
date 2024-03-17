# Databricks notebook source
# MAGIC %md
# MAGIC # Copy all tables & schemas into new catalog
# MAGIC Blog post: https://medium.com/@stefanko-ch/how-to-copying-all-schemas-and-tables-from-one-catalog-to-another-catalog-in-databricks-68f4fcc18223

# COMMAND ----------

# DBTITLE 1,Add Widgets
dbutils.widgets.text("source_catalog", "")
dbutils.widgets.text("target_catalog", "")

# COMMAND ----------

source_catalog = dbutils.widgets.get("source_catalog")
target_catalog = dbutils.widgets.get("target_catalog")

# COMMAND ----------

# DBTITLE 1,Create Target Catalog
df_tables = spark.read.table(f"{source_catalog}.information_schema.tables")
# display(df_tables)

# COMMAND ----------

# DBTITLE 1,Create copy function
def copy_table(source_catalog, target_catalog, schema, table):
    spark.sql(f"""CREATE SCHEMA IF NOT EXISTS {target_catalog}.{schema};""")
    
    if spark.catalog.tableExists(f"{target_catalog}.{schema}.{table}"):
        print(f"Skip table {target_catalog}.{schema}.{table}, because the table already exists.")
    
    else:
        print(f"Copy from {source_catalog}.{schema}.{table} into {target_catalog}.{schema}.{table}.")
        source_df = spark.table(f"{source_catalog}.{schema}.{table}")
    
        renamed_cols = [col.replace(" ", "_") for col in source_df.columns]

        renamed_df = source_df.toDF(*renamed_cols)

        renamed_df.write.mode("overwrite").saveAsTable(f"{target_catalog}.{schema}.{table}")


# COMMAND ----------

# DBTITLE 1,copy all tables & schemas
for index, row in df_tables.toPandas().iterrows():
    schema = row[1]
    table = row[2]
    copy_table(source_catalog, target_catalog, schema, table)

# COMMAND ----------

# DBTITLE 1,exit the notebook
dbutils.notebook.exit("Success")
