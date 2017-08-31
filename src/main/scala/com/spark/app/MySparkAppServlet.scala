package com.spark.app

import org.scalatra._
import org.apache.spark.{ SparkContext, SparkConf }

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

}
