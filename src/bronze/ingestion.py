# Databricks notebook source
import json
from pyspark.sql import types
import delta


def import_schema(path):
    with open(path, "r") as open_file:
        schema = json.load(open_file)
    return schema


def table_exists(table, database='default'):
    count = (spark.sql(f"show tables from {database};")
                  .filter(f"tableName = '{table}'")
                  .count())
    return count > 0


def upsert(df, df_delta, table):
    
    df.createOrReplaceGlobalTempView(f"tabnews_{table}")
    
    query = f'''select *
                from global_temp.tabnews_{table}
                where id is not null
                qualify row_number() over (partition by id, owner_id order by updated_at desc) = 1'''
    
    df_upsert = spark.sql(query)
    
    (df_delta.alias("d")
             .merge(df_upsert.alias("u"), "d.id = u.id and d.owner_id = u.owner_id")
             .whenNotMatchedInsertAll()
             .whenMatchedUpdateAll()
             .execute())
    

# COMMAND ----------

database = "bronze.tabnews"
table = "contents"
database_table = f"{database}.{table}"
raw_path = f"/mnt/datalake/raw/tabnews/{table}/*"
checkpoint_path = f"/mnt/datalake/raw/tabnews/checkpoint_{table}/"

schema = import_schema(f"{table}.json")
schema = types.StructType.fromJson(schema)

# COMMAND ----------

if not table_exists(table, database):
    df = spark.createDataFrame([], schema=schema)
    (df.write
       .format("delta")
       .mode("overwrite")
       .saveAsTable(f"{database}.{table}"))
    dbutils.fs.rm(checkpoint_path, True)

df_delta = delta.DeltaTable.forName(spark, f"{database}.{table}")

# COMMAND ----------

df_stream = (spark.readStream
                  .format("cloudFiles")
                  .schema(schema)
                  .option("cloudFiles.format", "json")
                  .load(raw_path))

# COMMAND ----------

stream =(df_stream.writeStream
                  .trigger(once=True)
                  .option("checkpointLocation", checkpoint_path)
                  .foreachBatch(lambda df, batchId: upsert(df, df_delta, table))
                  .start())
