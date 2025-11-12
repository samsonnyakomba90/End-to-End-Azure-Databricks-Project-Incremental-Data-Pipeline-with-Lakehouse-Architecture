import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
  name = "gold.businessAnalysis"
)
def businessAnalysis():
    df_cust = dlt.read("dim_customers")
    df_prod = dlt.read("dim_products")
    df_fact = dlt.read("fact_transactions")
    df = df_fact.join(df_cust, df_fact.customer_id == df_cust.customer_id, "inner").join(df_prod, df_fact.product_id == df_prod.product_id, "inner")
    df_final = df.withColumnRenamed("total_price","unit_price")\
                 .withColumn("total_amount", col("quantity") * col("unit_price"))\
                 .select("transaction_date","customer_name","product_name","category","region","quantity","unit_price","total_amount")\
                 .drop("customer_id", "product_id","transaction_id","price","Price")
    return df_final
