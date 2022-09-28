# Databricks notebook source
df = spark.read.format("parquet").load("dbfs:/FileStore/data.parquet")

# COMMAND ----------

display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

deduplicated_df = df.dropDuplicates(['transaction_timestamp','from_address','to_address'])

# COMMAND ----------

display(deduplicated_df)

# COMMAND ----------

deduplicated_df.count()

# COMMAND ----------

df.rdd.getNumPartitions()


# COMMAND ----------

deduplicated_df.rdd.getNumPartitions()

# COMMAND ----------

deduplicated_df.coalesce(1).write.format("delta").mode("overwrite").save("dbfs:/FileStore/blockchain__1_delta")

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/blockchain__1_delta

# COMMAND ----------

deduplicated_df.write.format("delta").mode("overwrite").saveAsTable("blockchain_delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table blockchain_delta

# COMMAND ----------



df_out1 = spark.sql("select token_id,amount,load_timestamp as date from (select token_id,amount,load_timestamp,row_number() over(partition by token_id order by amount desc) as rownum from blockchain_delta) a where a.rownum=1 order by amount desc limit 5")




# COMMAND ----------

display(df_out1)


# COMMAND ----------

df_out2 = spark.sql("select token_id,accumulatedtransactionamount,load_timestamp as date from (select token_id,amount,load_timestamp, accumulatedtransactionamount ,row_number() over(partition by token_id order by amount desc) as rownum from (SELECT token_id,load_timestamp,amount,SUM(amount) OVER (PARTITION BY token_id ORDER BY amount ) as accumulatedtransactionamount  FROM blockchain_delta) a) b where b.rownum=1 order by accumulatedtransactionamount desc limit 5")


# COMMAND ----------

display(df_out2)

# COMMAND ----------

df_out1.coalesce(1).write.format("parquet").mode("overwrite").save("dbfs:/FileStore/df_out1.parquet")

# COMMAND ----------

df_out1_parquet = spark.read.format("parquet").load("dbfs:/FileStore/df_out1.parquet")

# COMMAND ----------

display(df_out1_parquet)

# COMMAND ----------

df_out2.coalesce(1).write.format("parquet").mode("overwrite").save("dbfs:/FileStore/df_out2.parquet")

# COMMAND ----------

df_out2_parquet = spark.read.format("parquet").load("dbfs:/FileStore/df_out2.parquet")

# COMMAND ----------

display(df_out2_parquet)

# COMMAND ----------

