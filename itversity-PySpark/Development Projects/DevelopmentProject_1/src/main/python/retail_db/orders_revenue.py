from pyspark import SparkConf, SparkContext
import configparser as cp
import sys

# Run such Non-REPL Projects on a cluster using spark-submit

props = cp.RawConfigParser()
props.read("src/main/resources/application.properties")
env = sys.argv[1]

conf = SparkConf().\
    setMaster(props.get(env,"executionMode")).\
    setAppName("Orders Revenue").\
    set("spark.ui.port","4041")

sc = SparkContext(conf=conf)

orders = sc.textFile(props.get(env,"input.base.dir") + '/orders')
ordersMap = orders.filter(lambda x: x.split(',')[3] in ('COMPLETE', 'CLOSED'))
ordersKV = orders.map(lambda x: kv1(x))
def kv1(x):
    temp = x.split(',')
    return ((int(temp[0]), temp[1]))

orderItems = sc.textFile(props.get(env,"input.base.dir") + '/order_items')
orderItemsKV = orderItems.map(lambda x : kv2(x))
def kv2(x):
    temp = x.split(',')
    return((int(temp[1]),float(temp[4])))

jRDD = ordersKV.join(orderItemsKV)
jMap = jRDD.map(lambda x : x[1])
jRed = jMap.reduceByKey(lambda x,y : x+y)
jSort = jRed.sortByKey(True)

jSort.saveAsTextFile(props.get(env,"output.base.dir") + '/Revenues')