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

from pyspark.sql.types import IntegerType,FloatType

ordersDF = orders.withColumn('order_id',orders.order_id.cast(IntegerType())).\
                  withColumn('order_customerID',orders.order_customerID.cast(IntegerType()))

ordersDF.printSchema()
ordersDF.describe().show()
ordersDF.show()


order_items = spark.read.csv("/Users/shreyas_rl/Desktop/git/itversity-data/retail_db/order_items", inferSchema = True).\
              toDF('order_item_id','order_item_order_id','order_item_product_id','order_item_qty',
                   'order_item_subtotal','order_item_price')
order_items.printSchema()
order_items.describe().show()
order_items.show(5)