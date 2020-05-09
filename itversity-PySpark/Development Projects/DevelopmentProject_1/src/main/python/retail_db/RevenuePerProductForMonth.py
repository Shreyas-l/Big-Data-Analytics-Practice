from pyspark import SparkConf, SparkContext
import configparser as cp
import sys

props = cp.RawConfigParser()
props.read("src/main/resources/application.properties")


conf = SparkConf().\
    setMaster(props.get(sys.argv[4],"executionMode")).\
    setAppName("Total Revenue Per Day").\
    set("spark.ui.port","4041")

sc = SparkContext(conf=conf)
inputPath = sys.argv[1]
outputPath = sys.argv[2]
month = sys.argv[3]

"""
Path = sc._gateway.jvm.org.apache.hadoop.fs.path.Path
FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration

fs = FileSystem.get(Configuration())

if fs.exists(Path(inputPath)) == False:
    print("Input Path Does not Exist!")
else:
    if fs.exists(Path(outputPath)):
        fs.delete(Path(outputPath), True)"""

ordersRDD = sc.textFile(inputPath + "/orders")
orRDD = ordersRDD.filter(lambda x : month in x.split(",")[1]).\
        map(lambda x : (int(x.split(",")[0]),1))

orderItems = sc.textFile(inputPath + "/order_items")
orItRDD = orderItems.map(lambda x : (int(x.split(",")[0]), int(x.split(",")[2]), float(x.split(",")[4]) )).\
        join(orRDD).\
        map(lambda x : x[1]).\
            reduceByKey(lambda x,y : x+y)

products = open(inputPath + "/products/part-00000")
productRDD = sc.parallelize(products.read().splitlines()).\
        map(lambda x : (int(x.split(",")[0]),x.split(",")[2])).\
        join(orItRDD).\
        map(lambda x : str(x[1][0]) + "  " + str(x[1][1])).\
        saveAsTextFile(outputPath + "/revenues")

