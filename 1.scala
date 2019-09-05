val r = (1 to 10)
//r: scala.collection.immutable.Range.Inclusive = Range(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
val l = r.toList
//l: List[Int] = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
l(0)
//1
l.size
//res1: Int = 10
for(i <- l) println(i)
//or below	
l.foreach(println)	
//l.take(5) gives a list with first 5 elements
l.take(5).foreach(println)
l.filter // hit double tab , will give syntax,  annon fun  which takes each element of l of type Int as p and return true condtion elem
//def filter(p: Int => Boolean): List[Int]
l.filter(p=> {p%2==0})
//res14: List[Int] = List(2, 4, 6, 8, 10)

/*
df1=spark.read.json("s3://zz-testing/kik/tmp/test/retail_db_json/order_items/")

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
*/


//GetRevenueForOrder -> get the rev for orderid=2 from file /data/retail_db/order_items/part-000000

package retail_db_list
object GetRevenueForGivenOrder{
	
	def main(args: Array[String]): Unit = {

		val orderItemsFileLoc = args(0)
		val orderId =args(1).toInt
		val orderItems=scala.io.Source.fromFile(orderItemsFileLoc).getLines().toList
		val filteredorderItems=orderItems.filter(e => {e.split(",")(1).toInt == orderId})
		//filteredorderItems.foreach(println)
		val orderItemsSubtotals=filteredorderItems.map(e=>{e.split(",")(5).toFloat})
		val revForGivenOrder=orderItemsSubtotals.reduce((curr,nxt)=>{curr+nxt})
		println("rev for a given orderid:" + orderId +" is:" +revForGivenOrder)

	}
}


//RDD- distributed fault tolerant collection
//spark-submit --master yarn --deploy-mode client --class retail_db_list.GetRevenueForGivenOrderSprkParallelize GetRevenueForGivenOrderSprkParallelize.jar /data/retail_db/order_items/part-0000 2
data= (1 to 100).toList
dataRDD=sc.parallelize(data)

package retail_db_list
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf,SparkContext}

object GetRevenueForGivenOrderSprkParallelize{
	
	def main(args: Array[String]): Unit = {

		val orderItemsFileLoc = args(0)
		val orderId =args(1).toInt
		val spark= SparkSession.builder()
                                .appName("GetRevenueForGivenOrderFromAfileLocation")
                                .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                                .enableHiveSupport()
                                .getOrCreate()
        spark.conf.set("hive.mapred.mode","nonstrict")
        spark.conf.set("hive.exec.dynamic.partition.mode","nonstrict")
        spark.conf.set("hive.exec.dynamic.partition", "true")
        spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
        spark.conf.set("fs.s3.enableServerSideEncryption", "true")
        spark.conf.set("spark.hadoop.hive.exec.max.dynamic.partitions", "50000")
        spark.conf.set("spark.hadoop.hive.exec.max.dynamic.partitions.pernode", "50000")
        spark.sql("set spark.sql.caseSensitive=false")
                                
		val orderItemsList=scala.io.Source.fromFile(orderItemsFileLoc).getLines().toList
		val orderItems=sc.parallelize(orderItemsList)
		//orderItems.toDebugString.lines.foreach(println) //will give the graph steps dag
		val filteredorderItems=orderItems.filter(e => {e.split(",")(1).toInt == orderId})
		//filteredorderItems.foreach(println)
		val orderItemsSubtotals=filteredorderItems.map(e=>{e.split(",")(5).toFloat})
		val revForGivenOrder=orderItemsSubtotals.reduce((curr,nxt)=>{curr+nxt})
		println("rev for a given orderid:" + orderId +" is:" +revForGivenOrder)

	}
}




























/*
#no of tasks=no of partitions in spark, block size is 128MB,so if we have 30 files,each file is 1gb
#each file will have 9 blocks-> total 30*9=270 tasks by default
sc.textFile(/public/filesfolder).count
hdfs fsck /public/filesfolder -files -blocks
#spark creates one partition for each block of a file (block size being 128MB), you can ask for more partitions,but you cannot have fewer partitions than blocks
#sc.textFile(/public/filesfolder,540).count

*/

val orderItems= sc.textFile("s3://zz-testing/kik/tmp/test/retail_db/order_items")
//orderItems: org.apache.spark.rdd.RDD[String] = s3://zz-testing/kik/tmp/test/retail_db/order_items MapPartitionsRDD[1] at textFile at <console>:24

val orderItems1= spark.read.textFile("s3://zz-testing/kik/tmp/test/retail_db/order_items")
//orderItems1: org.apache.spark.sql.Dataset[String] = [value: string]

orderItems.take(5).foreach(println)
/*
1,1,957,1,299.98,299.98                                                         
2,2,1073,1,199.99,199.99
3,2,502,5,250.0,50.0
4,2,403,1,129.99,129.99
5,4,897,2,49.98,24.99*/
orderItems1.take(5).foreach(println)
/*1,1,957,1,299.98,299.98                                                         
2,2,1073,1,199.99,199.99
3,2,502,5,250.0,50.0
4,2,403,1,129.99,129.99
5,4,897,2,49.98,24.99*/

//gerRevfor each orderid

val orderItemsMap = orderItems.map(e=>{(e.split(",")(1).toInt,e.split(",")(4).toFloat)})
//orderItemsMap: org.apache.spark.rdd.RDD[(Int, Float)] = MapPartitionsRDD[6] at map at <console>:25
orderItemsMap.take(5).foreach(println)
/*
(1, 299.98)                                                                     
(2, 199.99)
(2, 250.0)
(2, 129.99)
(4, 49.98)
*/
val revPerOrder=orderItemsMap.reduceByKey((curr,nxt)=>curr+nxt)

//revPerOrder: org.apache.spark.rdd.RDD[(Int, Float)] = ShuffledRDD[9] at reduceByKey at <console>:25

revPerOrder.take(5).foreach(println)

(41234,109.94)                                                                  
(65722,1319.8899)
(28730,349.95)
(68522,329.99)
(23776,329.98)

//get rev for orderid 2

val revPerOrderid2=revPerOrder.filter(e=>e._1==2).first
//revPerOrderid2: (Int, Float) = (2,579.98)   































