from pyspark.sql import SparkSession

spark = SparkSession.\
        builder.\
        master('local').\
        appName('Getting Started').\
        getOrCreate()

#sc = spark.sparkContext()   If you want to leverage some core sc functionality

orders = spark.read.csv('/Users/shreyas_rl/Desktop/git/itversity-data/retail_db/orders').\
         toDF('order_id','order_date','order_customerID','order_status')

orders.printSchema()
orders.describe().show()
orders.show()

from pyspark.sql.types import IntegerType

ordersDF = orders.withColumn('order_id',orders.order_id.cast(IntegerType())).\
                  withColumn('order_customerID',orders.order_customerID.cast(IntegerType()))

ordersDF.printSchema()
ordersDF.describe().show()
ordersDF.show()