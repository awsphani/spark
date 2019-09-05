l=range(1,10)
type(l)
#<type 'list'>
l
#[1, 2, 3, 4, 5, 6, 7, 8, 9]
l[0]
#1
len(l)
#9
for i in l:
	print i
#list with first 5 elements
l[:5]
#list comphrension find even numbers
[i for i in l if i%2==0]
#find even numbers using filter function
filter(lambda p: p%2==0,l)
#[2, 4, 6, 8]
'''
df1=spark.read.json("s3://zz-testing/kik/tmp/test/retail_db_json/order_items/")
df1.show(5,False)
+-------------+-------------------+---------------------+------------------------+-------------------+-------------------+
|order_item_id|order_item_order_id|order_item_product_id|order_item_product_price|order_item_quantity|order_item_subtotal|
+-------------+-------------------+---------------------+------------------------+-------------------+-------------------+
|1            |1                  |957                  |299.98                  |1                  |299.98             |
|2            |2                  |1073                 |199.99                  |1                  |199.99             |
|3            |2                  |502                  |50.0                    |5                  |250.0              |
|4            |2                  |403                  |129.99                  |1                  |129.99             |
|5            |4                  |897                  |24.99                   |2                  |49.98              |
+-------------+-------------------+---------------------+------------------------+-------------------+-------------------+

oi.txt

1,1,100,299.98,1,299.98
2,2,101,199.99,1,199.99
3,2,102,50.0,5,250.0
4,2,103,129.99.0,1,129.99
5,4,105,24.99,2,49.98

'''

#GetRevenueForOrder -> get the rev for orderid=2 from file /data/retail_db/order_items/part-000000
#python rev.py --orderItemsFileLoc /home/hadoop/oi.txt --orderId 2

import logging
import argparse
logger = logging.getLogger("GetRevenueForGivenOrderFromAfileLocation")
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())
class GetRevenueForGivenOrder:
        def __init__(self, **argsv):
                logger.info('GetRevenueForGivenOrderFromAfileLocation')
                self.orderItemsFileLoc = argsv['orderItemsFileLoc'].lower()
                self.orderId = argsv['orderId'].lower()
                logger.info("orderItemsFileLoc  : {}".format(self.orderItemsFileLoc))
                logger.info("orderId            : {}".format(self.orderId))
        def calcRevenueForGivenOrder(self):
                f=open(self.orderItemsFileLoc)
                contents=f.read()
                orderItems=contents.splitlines()
                orderId=int(self.orderId)
                filteredorderItems=filter(lambda e: int(e.split(",")[1])==orderId,orderItems)
                orderItemsSubtotals=map(lambda e: float(e.split(",")[5]),filteredorderItems)
                revForGivenOrder=reduce(lambda curr,nxt: curr+nxt,orderItemsSubtotals)
                logger.info("Rev for a given orderid:{0} is {1}".format(self.orderId,revForGivenOrder))
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="input of arguments for GetRevenueForGivenOrder ")
    parser.add_argument('-orderItemsFileLoc', '--orderItemsFileLoc', help="orderItemsFileLoc")
    parser.add_argument('-orderId', '--orderId', default='2')
    argsv = vars(parser.parse_args())
    rev = GetRevenueForGivenOrder(**argsv)
    rev.calcRevenueForGivenOrder()


#RDD- distributed fault tolerant collection

data=range(1,100)
dataRDD=sc.parallelize(data)



import logging
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, LongType, BooleanType
logger = logging.getLogger("GetRevenueForGivenOrderFromAfileLocation")
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())
class GetRevenueForGivenOrder:
        def __init__(self, **argsv):
                logger.info('GetRevenueForGivenOrderFromAfileLocation')
                self.orderItemsFileLoc = argsv['orderItemsFileLoc'].lower()
                self.orderId = argsv['orderId'].lower()
                logger.info("orderItemsFileLoc  : {}".format(self.orderItemsFileLoc))
                logger.info("orderId            : {}".format(self.orderId))
                self.spark= SparkSession.builder\
                                .appName("GetRevenueForGivenOrderFromAfileLocation")\
                                .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")\
                                .enableHiveSupport()\
                                .getOrCreate()

		        # Turn hive strict mode to off so, we can write into partitioned table
		        self.spark.conf.set("hive.mapred.mode","nonstrict")
		        self.spark.conf.set("hive.exec.dynamic.partition.mode","nonstrict")
		        self.spark.conf.set("hive.exec.dynamic.partition", "true")
		        self.spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
		        self.spark.conf.set("fs.s3.enableServerSideEncryption", "true")
		        self.spark.conf.set("spark.hadoop.hive.exec.max.dynamic.partitions", "50000")
		        self.spark.conf.set("spark.hadoop.hive.exec.max.dynamic.partitions.pernode", "50000")
		        self.spark.sql("set spark.sql.caseSensitive=false")


        def calcRevenueForGivenOrder(self):
                f=open(self.orderItemsFileLoc)
                contents=f.read()
                orderItemsList=contents.splitlines()
                orderItems=sc.parallelize(orderItemsList)
                orderId=int(self.orderId)
                filteredorderItems=filter(lambda e: int(e.split(",")[1])==orderId,orderItems)
                orderItemsSubtotals=map(lambda e: float(e.split(",")[5]),filteredorderItems)
                revForGivenOrder=reduce(lambda curr,nxt: curr+nxt,orderItemsSubtotals)
                logger.info("Rev for a given orderid:{0} is {1}".format(self.orderId,revForGivenOrder))
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="input of arguments for GetRevenueForGivenOrder ")
    parser.add_argument('-orderItemsFileLoc', '--orderItemsFileLoc', help="orderItemsFileLoc")
    parser.add_argument('-orderId', '--orderId', default='2')
    argsv = vars(parser.parse_args())
    rev = GetRevenueForGivenOrder(**argsv)
    rev.calcRevenueForGivenOrder()

#no of tasks=no of partitions in spark, block size is 128MB,so if we have 30 files,each file is 1gb
#each file will have 9 blocks-> total 30*9=270 tasks by default
#sc.textFile(/public/filesfolder).count()
#hdfs fsck /public/filesfolder -files -blocks

#spark creates one partition for each block of a file (block size being 128MB), you can ask for more partitions,but you cannot have fewer partitions than blocks

#sc.textFile(/public/filesfolder,540).count()

#<class 'pyspark.rdd.RDD'>
orderItems1= spark.read.textFile("s3://zz-testing/kik/tmp/test/retail_db/order_items")
#AttributeError: 'DataFrameReader' object has no attribute 'textFile'

orderItems1= spark.read.json("s3://zz-testing/kik/tmp/test/retail_db_json/order_items")
type(orderItems1)
#<class 'pyspark.sql.dataframe.DataFrame'>

orderItems= sc.textFile("s3://zz-testing/kik/tmp/test/retail_db/order_items")
type(orderItems)


for i in orderItems.take(5):
	print i
'''
1,1,957,1,299.98,299.98                                                         
2,2,1073,1,199.99,199.99
3,2,502,5,250.0,50.0
4,2,403,1,129.99,129.99
5,4,897,2,49.98,24.99
'''

#gerRev for each orderid

orderItemsMap=orderItems.map(lambda e:(int(e.split(",")[1]),float(e.split(",")[4])))
type(orderItemsMap)
#<class 'pyspark.rdd.PipelinedRDD'>
for i in orderItemsMap.take(5): print i

'''
(1, 299.98)                                                                     
(2, 199.99)
(2, 250.0)
(2, 129.99)
(4, 49.98)
'''

revPerOrder=orderItemsMap.reduceByKey(lambda curr,nxt:curr+nxt)

for i in revPerOrder.take(5): print i
'''
(2, 579.98)                                                                     
(4, 699.85)
(8, 729.8399999999999)
(10, 651.9200000000001)
(12, 1299.8700000000001)
'''

#get rev for orderid 2

revPerOrderid2=revPerOrder.filter(lambda e:e[0]==2)
revPerOrderid2.first()
#(2, 579.98)









































