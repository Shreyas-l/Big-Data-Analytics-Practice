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


order_items = spark.read.csv("/Users/shreyas_rl/Desktop/git/itversity-data/retail_db/order_items").\
              toDF('order_item_id','order_item_order_id','order_item_product_id','order_item_qty',
                   'order_item_subtotal','order_item_price')
order_items.printSchema()
order_items.describe().show()
order_items.show(5)

order_itemsDF = order_items.withColumn('order_item_id',order_items.order_item_id.cast(IntegerType())).\
                            withColumn('order_item_order_id',order_items.order_item_order_id.cast(IntegerType())).\
                            withColumn('order_item_product_id',order_items.order_item_product_id.cast(IntegerType())).\
                            withColumn('order_item_qty',order_items.order_item_qty.cast(IntegerType())).\
                            withColumn('order_item_subtotal',order_items.order_item_subtotal.cast(FloatType())).\
                            withColumn('order_item_price',order_items.order_item_price.cast(FloatType()))

order_itemsDF.printSchema()
order_itemsDF.show(5)