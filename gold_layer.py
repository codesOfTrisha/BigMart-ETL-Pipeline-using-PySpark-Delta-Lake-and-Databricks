# Databricks notebook source
df=spark.table("BigMart_silver")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sales by Outlet Type

# COMMAND ----------

from pyspark.sql.functions import sum
sales_by_Outlet_Type=df.groupBy('Outlet_Type').agg(sum('Item_Outlet_Sales').alias('Total_Sales'))

display(sales_by_Outlet_Type)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sales by Item Type
# MAGIC

# COMMAND ----------

Sales_by_Item_Type=df.groupBy('Item_Type').agg(sum('Item_Outlet_Sales').alias('Total_Sales'))
display(Sales_by_Item_Type)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sales by City Tier

# COMMAND ----------

Sales_by_City_Tier=df.groupBy('Outlet_Location_Type').agg(sum('Item_Outlet_Sales').alias('Total_Sales'))
display(Sales_by_City_Tier)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Outlet Performance Dashboard

# COMMAND ----------

from pyspark.sql.functions import sum, avg, count

outletr_dashboard = df.groupBy(
    "Outlet_Identifier",
    "Outlet_Type"
).agg(
    sum("Item_Outlet_Sales").alias("Total_Sales"),
    avg("Item_Outlet_Sales").alias("Average_Sales"),
    count("*").alias("Total_Counts")
)

display(outletr_dashboard)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top 10 Products

# COMMAND ----------

from pyspark.sql.functions import desc
Top_Products=df.groupBy('Item_Identifier').agg(sum('Item_outlet_Sales')\
                .alias('Total_Sales'))\
                .orderBy(desc('Total_Sales'))
display(Top_Products.limit(10))
                                                                                            

# COMMAND ----------

# MAGIC %md
# MAGIC ## Profit Analysis

# COMMAND ----------

Profile_Analysis=df.groupBy('Outlet_Type').agg(sum('Estimated_Profit').alias('Total_Profit'))
display(Profile_Analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final KPI Summary Table

# COMMAND ----------

kpi_summary = df.agg(
    sum("Item_Outlet_Sales").alias("Total_Sales"),
    avg("Item_Outlet_Sales").alias("Average_Sales"),
    sum("Estimated_Profit").alias("Total_Profit"),
    count("*").alias("Total_Records")
)

display(kpi_summary)

# COMMAND ----------

sales_by_Outlet_Type.write.mode('overwrite').format('delta')\
    .saveAsTable('Gold_Sales_by_Outlet_Type')
Sales_by_Item_Type.write.mode('overwrite').format('delta')\
    .saveAsTable('Gold_Sales_by_Item_Type')
Sales_by_City_Tier.write.mode('overwrite').format('delta')\
    .saveAsTable('Gold_Sales_by_City_Tier')
outletr_dashboard.write.mode('overwrite').format('delta')\
    .saveAsTable('Gold_Outlet_Dashboard')
Top_Products.write.mode('overwrite').format('delta')\
    .saveAsTable('Gold_Top_Products')
Profile_Analysis.write.mode('overwrite').format('delta')\
    .saveAsTable('Gold_Profile_Analysis')

# COMMAND ----------

kpi_summary.write.format('delta')\
           .mode('overwrite')\
            .saveAsTable('Gold_KPI_Summary')

# COMMAND ----------

spark.sql("SHOW TABLES").show(truncate=False)