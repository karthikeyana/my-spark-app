package com.spark.app

import org.scalatra._
import org.apache.spark.{ SparkContext, SparkConf }

import java.io.File
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

case class Record(key: Int, value: String)

class MySparkAppServlet extends MySparkAppStack {

  get("/") {
    <html>
      <body>
        <h1>Hello, world!</h1>
        Say <a href="hello-scalate">hello to Scalate</a>.
      </body>
    </html>
  }

  get("/wc") {
		val inputFile = "/home/limitless/Documents/projects/test/my-spark-app/README.md"
		val outputFile = "/home/limitless/Documents/projects/test/my-spark-app/README.txt"
		val conf = new SparkConf().setAppName("wordCount").setMaster("local[*]")
		val sc = new SparkContext(conf)
		val input =  sc.textFile(inputFile)
		val words = input.flatMap(line => line.split(" "))
		val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
		counts.saveAsTextFile(outputFile)
  	}

  get("/sh") {
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    sql("LOAD DATA LOCAL INPATH '/home/limitless/Documents/projects/test/my-spark-app/kv1.txt' INTO TABLE src")
    sql("SELECT * FROM src").show()
    sql("SELECT COUNT(*) FROM src").show()

    val sqlDF = sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")
    val stringsDS = sqlDF.map {
      case Row(key: Int, value: String) => s"Key: $key, Value: $value"
    }
    stringsDS.show()
    val recordsDF = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
    recordsDF.createOrReplaceTempView("records")
    sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show()
    spark.stop()
  }

}
