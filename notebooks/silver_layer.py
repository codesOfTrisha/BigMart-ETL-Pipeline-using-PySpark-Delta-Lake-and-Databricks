# Databricks notebook source
# MAGIC %md
# MAGIC # Clean and Transform Data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

df=spark.read.format('csv')\
            .option('header','true')\
            .option('inferSchema','true')\
            .load('/Volumes/databricksrajanya/default/databricks_rajanya/BigMart Sales.csv')

# COMMAND ----------

display(df.limit(5))

# COMMAND ----------

from pyspark.sql.functions import col
df.select(col('Item_Fat_Content')).distinct().show()
df.select(col('Item_Type')).distinct().show()
df.select(col('Outlet_Size')).distinct().show()
df.select(col('Outlet_Location_Type')).distinct().show()
df.select(col('Outlet_Type')).distinct().show()

# COMMAND ----------

print("Total Rows:", df.count())

print("Unique Rows:", df.dropDuplicates().count())

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Null Handling

# COMMAND ----------

# MAGIC %md
# MAGIC ### • Filling Item_Weight with average

# COMMAND ----------

from pyspark.sql.functions import avg
avg_weight=df.select(avg('Item_Weight')).collect()[0][0]
df=df.fillna(avg_weight,subset=['Item_Weight'])

# COMMAND ----------

df.filter(col('Item_Weight').isNull()).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### • Fill Outlet_Size with 'Unknown'

# COMMAND ----------

df=df.fillna('Unknown',subset=['Outlet_Size'])
df.filter(col('Outlet_Size').isNull()).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Standardization of Categorical Values

# COMMAND ----------

# MAGIC %md
# MAGIC ### • Standardize Item_Fat_Content

# COMMAND ----------

from pyspark.sql.functions import when,col
df=df.withColumn('Item_Fat_Content',
                 when(col('Item_Fat_Content').isin('LF','low fat','Low Fat'),'Low Fat')
                 .when(col('Item_Fat_Content').isin('reg','Regular'),'Regular')
                 .otherwise(col("Item_Fat_Content"))
)
df.select('Item_Fat_Content').distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### • Apply initcap() on Item_Type

# COMMAND ----------

from pyspark.sql.functions import initcap
df=df.withColumn('Item_Type',initcap(col('Item_Type')))
df.select('Item_Type').distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Derived Business Columns

# COMMAND ----------

# MAGIC %md
# MAGIC ### • GST = Item_MRP * 0.18

# COMMAND ----------

from pyspark.sql.functions import *
df=df.withColumn('GST', col("Item_MRP") * 0.18)
df.select('Item_MRP','GST').show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### • Final_Price = Item_MRP + GST

# COMMAND ----------

from pyspark.sql.functions import col
df=df.withColumn('Final_Price',col("Item_MRP")+col("GST"))
df.select('Item_MRP','GST','Final_Price').show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Outlet age

# COMMAND ----------

df=df.withColumn('Outlet_Age',year(current_date())-col("Outlet_Establishment_Year"))
df.select('Outlet_Establishment_Year','Outlet_Age').show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC ## ### • Assume 20% Profit Margin

# COMMAND ----------

df=df.withColumn('Estimated_Profit',col("Item_Outlet_Sales")*0.20)
df.select('Item_Outlet_Sales','Estimated_Profit').show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Profit Category

# COMMAND ----------

from pyspark.sql.functions import when

df = df.withColumn(
    "Profit_Category",
    when(col("Estimated_Profit") > 600, "High Profit")
    .when(col("Estimated_Profit") > 300, "Medium Profit")
    .otherwise("Low Profit")
)
df.select('Estimated_Profit','Profit_Category').show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mention Price_Band

# COMMAND ----------

df=df.withColumn('Price_Band',when(col("Item_MRP")>200,"Premium")
                 .otherwise("Budget"))
df.select('Item_MRP','Price_Band').show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Sale Category

# COMMAND ----------

df=df.withColumn('Sale_Category',when(col('Item_Outlet_Sales')>3000,"High Sale")
                 .when(col('Item_Outlet_Sales')>1500,"Medium Sale")
                 .otherwise("Low Sale"))

df.select('Item_Outlet_Sales','Sale_Category').show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit Columns
# MAGIC ### • Load_Timestamp using current_timestamp()

# COMMAND ----------

df=df.withColumn('Load_Timestamp',current_timestamp())
df.select('Load_Timestamp').show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### • Data_Source using lit('BigMart CSV')

# COMMAND ----------

df=df.withColumn('Data_Source',lit("BigMart CSV"))
df.select('Data_Source').show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Window Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rank Products by Sales Within Each Outlet

# COMMAND ----------

from pyspark.sql.functions import rank, col
from pyspark.sql.window import Window
df=df.withColumn('Sales_Rank',rank().over(Window.partitionBy('Outlet_Identifier')\
                            .orderBy(col('Item_Outlet_Sales').desc())))

df.select('Outlet_Identifier','Item_Outlet_Sales','Sales_Rank')\
    .show(5000)                          

# COMMAND ----------

from pyspark.sql.functions import when, col

df = df.withColumn(
    "Top_Seller",
    when(col("Sales_Rank") == 1, "Yes")
    .otherwise("No")
)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Validation Checks

# COMMAND ----------

from pyspark.sql.functions import col
for c in df.columns:
    print(c,df.filter(col(c).isNull()).count())

# COMMAND ----------

df.select("Item_Fat_Content").distinct().show()

# COMMAND ----------

df.select("Item_Type").distinct().show()

# COMMAND ----------

df.select(
    "GST",
    "Final_Price",
    "Outlet_Age",
    "Price_Band",
    "Sale_Category"
).show(10)

# COMMAND ----------

df.select(
    "Outlet_Identifier",
    "Item_Outlet_Sales",
    "Sales_Rank"
).orderBy(
    "Outlet_Identifier",
    "Sales_Rank"
).show(20)

# COMMAND ----------

df.write.format('delta')\
    .mode("overwrite")\
    .saveAsTable("BigMart_silver")

spark.table("bigmart_silver").show(5)
