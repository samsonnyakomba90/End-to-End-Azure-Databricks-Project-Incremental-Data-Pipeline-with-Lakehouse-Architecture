import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ------PRODUCTS ----------------
@dlt.table(
    name = 'stg_products'
)
def stg_products():
    df = spark.read.format("delta")\
                   .load("/Volumes/streaming/bronze/bronzevolume/products/data")
    return df

# ------ DIMPRODUCTS ----------------
@dlt.table(
    name = 'dim_products'
)
@dlt.expect_all_or_drop({
    "product_id": "product_id IS NOT NULL",
    "price": "price IS NOT NULL AND price >= 0"
})
def dim_products():
    df = spark.readStream.table('stg_products')
    df = df.withColumnRenamed('ProductID','product_id')\
           .withColumnRenamed('ProductName','product_name')\
           .withColumnRenamed('Category','category')\
           .withColumnRenamed('Price','price')\
           .withColumn('Price',col('Price').cast('double'))\
           .drop("_rescued_data")\
           .dropDuplicates()     
    return df
