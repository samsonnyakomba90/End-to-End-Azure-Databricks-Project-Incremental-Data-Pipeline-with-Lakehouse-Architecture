import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ------CUSTOMERS ----------------
@dlt.table(
    name = 'stg_customers'
)
def stg_customers():
    df = spark.read.format("delta")\
                   .load("/Volumes/streaming/bronze/bronzevolume/customers/data")
    return df

# ------ DIMCUSTOMERS ----------------
@dlt.table(
    name = 'dim_customers'
)
# ------EXPECTATIONS ------------------
@dlt.expect_all_or_drop({
    "customer_id": "customer_id IS NOT NULL",
    "customer_name": "customer_name IS NOT NULL",
    "region": "region IS NOT NULL"
})
def dim_customers():
    df = spark.readStream.table('stg_customers')
    df = df.withColumnRenamed('CustomerID','customer_id')\
           .withColumnRenamed('CustomerName','customer_name')\
           .withColumnRenamed('Region','region')\
           .withColumnRenamed('SignupDate','signup_date')\
           .withColumn('signup_date',col('signup_date').cast('timestamp'))\
           .drop("_rescued_data")\
           .dropDuplicates()     
    return df
