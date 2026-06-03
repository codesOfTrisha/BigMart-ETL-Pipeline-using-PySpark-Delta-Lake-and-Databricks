# Databricks notebook source
# MAGIC %md
# MAGIC ###Read the file
# MAGIC

# COMMAND ----------

df=spark.read.format('csv')\
            .option('header','true')\
            .option('inferSchema','true')\
            .load('/Volumes/databricksrajanya/default/databricks_rajanya/BigMart Sales.csv')
display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Validation

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### NULL Analysis

# COMMAND ----------

from pyspark.sql.functions import col
df.filter(col('Item_Weight').isNull()).count()

# COMMAND ----------

for c in df.columns:
    print(c,df.filter(col(c).isNull()).count())