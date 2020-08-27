/**
  * (C) Copyright IBM Corp. 2015 - 2017
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  *     http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  *
  */

package com.ibm.sparktc.sparkbench.workload.rdd
import com.ibm.sparktc.sparkbench.utils.GeneralFunctions.{getOrDefault, time}
import com.ibm.sparktc.sparkbench.utils.SaveModes
import com.ibm.sparktc.sparkbench.workload.{Workload, WorkloadDefaults}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

case class ReviewResult(name: String, timestamp: Long, total_runtime: Long, pi_approximate: (Long, Long, Long))

object Review extends WorkloadDefaults {
  val name = "amazon-review"
  def apply(m: Map[String, Any]): Workload = {

    val input = m.get("review").map(_.asInstanceOf[String])
    val output = None
    val metadata = m.get("metadata").map(_.asInstanceOf[String])
    val cachePolicy = m.get("cache").map(_.asInstanceOf[String])

    cachePolicy.get match {
      case "All" =>  {
        println("##################  All ###############################\n")
        Review_All(input=input, output = output, metadata = metadata, cachePolicy = cachePolicy)
      }
      case "SODA" => {
        println("##################  SODA ###############################\n")
        Review_SODA(input=input, output = output, metadata = metadata, cachePolicy = cachePolicy)
      }
      case "None" => {
        println("##################  None ###############################\n")
        Review_None(input=input, output = output, metadata = metadata, cachePolicy = cachePolicy)
      }
      case _ => {
        Review_None(input=input, output = output, metadata = metadata, cachePolicy = cachePolicy)
      }
    }
  }
}


case class Review_All(input: Option[String] = None,
                      output: Option[String] = None,
                      saveMode: String = SaveModes.error,
                      metadata: Option[String] = None,
                      cachePolicy: Option[String]) extends Workload {

  private def execution(spark: SparkSession) = {

    val reviewRDD = spark.read.json(input.get)
      .rdd
      .map( row => {
        val asin = row.getString(0) // product key
        val overall = row.getDouble(2) // overall
        val count = row.getString(3).split(" ").length //total number of words in a comment
        (asin, overall, count)
      }).cache()

    /**
      * Task 1, calculate average words used in a comment for a asin starting with "B0000"
      */
    val averageComments = reviewRDD.map(row => {
      val asin = row._1
      val count = row._3 //total number of words in a comment
      (asin,count)
    }).cache()


    val a2 = averageComments.groupByKey().cache()


    val a3 = a2.map({
      case (asin, nums) =>
        val average = nums.sum * 1.0 / nums.size
        (asin, average)
    }).filter{ case (asin,_) => asin.contains("B000")}.cache()


    val reg1 = a3.count()

    /**
      * Task 2, Calculate average overall value for each asin.
      */
    val aggData = reviewRDD.map(
      row => (row._1,(row._2,row._3))
    ).cache()


    val b3 = aggData.groupByKey().map({
      case (asin,values) =>
        val average = values.map(_._1).sum / values.size
        (asin,average)
    }).cache()

    val reg2 = b3.count()

    val productRDD:RDD[Row] = spark.read.json(metadata.get).rdd.cache()

    /**
      * Task 3, Calcualte average overall value for each brand
      */
    val review = reviewRDD.map(row => (row._1,row._2)).cache() // (asin, overall)


    val meta = productRDD.map(row => (row.getString(0),row.getString(1))).cache() // (asin, brand)


    val reg = review.join(meta).map({
      case (_, (value, brand)) => (brand,value)
    }).groupByKey().map({
      case (brand, values) =>
        val average = values.sum * 1.0 / values.size
        (brand,average)
    }).cache()


    val reg3 = reg.count()

    (reg1, reg2, reg3)
  }

  override def doWorkload(df: Option[DataFrame] = None, spark: SparkSession): DataFrame = {
    val timestamp = System.currentTimeMillis()
    val (t, pi) = time(execution(spark))
    spark.createDataFrame(Seq(ReviewResult("review", timestamp, t, pi)))
  }
}


case class Review_SODA(input: Option[String] = None,
                      output: Option[String] = None,
                      saveMode: String = SaveModes.error,
                      metadata: Option[String] = None,
                      cachePolicy: Option[String]) extends Workload {

  private def execution(spark: SparkSession) = {

    val reviewRDD = spark.read.json(input.get)
      .rdd
      .map( row => {
        val asin = row.getString(0) // product key
        val overall = row.getDouble(2) // overall
        val count = row.getString(3).split(" ").length //total number of words in a comment
        (asin, overall, count)
      }).cache()

    /**
      * Task 1, calculate average words used in a comment for a asin starting with "B0000"
      */
    val averageComments = reviewRDD.map(row => {
      val asin = row._1
      val count = row._3 //total number of words in a comment
      (asin,count)
    })


    val a2 = averageComments.groupByKey()


    val a3 = a2.map({
      case (asin, nums) =>
        val average = nums.sum * 1.0 / nums.size
        (asin, average)
    }).filter{ case (asin,_) => asin.contains("B000")}


    val reg1 = a3.count()

    /**
      * Task 2, Calculate average overall value for each asin.
      */
    val aggData = reviewRDD.map(
      row => (row._1,(row._2,row._3))
    )


    val b3 = aggData.groupByKey().map({
      case (asin,values) =>
        val average = values.map(_._1).sum / values.size
        (asin,average)
    })

    val reg2 = b3.count()

    val productRDD:RDD[Row] = spark.read.json(metadata.get).rdd

    /**
      * Task 3, Calcualte average overall value for each brand
      */
    val review = reviewRDD.map(row => (row._1,row._2)) // (asin, overall)


    val meta = productRDD.map(row => (row.getString(0),row.getString(1))) // (asin, brand)


    val reg = review.join(meta).map({
      case (_, (value, brand)) => (brand,value)
    }).groupByKey().map({
      case (brand, values) =>
        val average = values.sum * 1.0 / values.size
        (brand,average)
    })


    val reg3 = reg.count()

    (reg1, reg2, reg3)
  }

  override def doWorkload(df: Option[DataFrame] = None, spark: SparkSession): DataFrame = {
    val timestamp = System.currentTimeMillis()
    val (t, pi) = time(execution(spark))
    spark.createDataFrame(Seq(ReviewResult("review", timestamp, t, pi)))
  }
}



case class Review_None(input: Option[String] = None,
                  output: Option[String] = None,
                  saveMode: String = SaveModes.error,
                  metadata: Option[String] = None,
                  cachePolicy: Option[String]) extends Workload {

  private def execution(spark: SparkSession) = {


    val reviewRDD = spark.read.json(input.get)
      .rdd
      .map( row => {
        val asin = row.getString(0) // product key
        val overall = row.getDouble(2) // overall
        val count = row.getString(3).split(" ").length //total number of words in a comment
        (asin, overall, count)
      })

    /**
      * Task 1, calculate average words used in a comment for a asin starting with "B0000"
      */
    val averageComments = reviewRDD.map(row => {
      val asin = row._1
      val count = row._3 //total number of words in a comment
      (asin,count)
    })


    val a2 = averageComments.groupByKey()


    val a3 = a2.map({
      case (asin, nums) =>
        val average = nums.sum * 1.0 / nums.size
        (asin, average)
    }).filter{ case (asin,_) => asin.contains("B000")}


    val reg1 = a3.count()

    /**
      * Task 2, Calculate average overall value for each asin.
      */
    val aggData = reviewRDD.map(
      row => (row._1,(row._2,row._3))
    )


    val b3 = aggData.groupByKey().map({
      case (asin,values) =>
        val average = values.map(_._1).sum / values.size
        (asin,average)
    })

    val reg2 = b3.count()

    val productRDD:RDD[Row] = spark.read.json(metadata.get).rdd

    /**
      * Task 3, Calcualte average overall value for each brand
      */
    val review = reviewRDD.map(row => (row._1,row._2)) // (asin, overall)


    val meta = productRDD.map(row => (row.getString(0),row.getString(1))) // (asin, brand)


    val reg = review.join(meta).map({
      case (_, (value, brand)) => (brand,value)
    }).groupByKey().map({
      case (brand, values) =>
        val average = values.sum * 1.0 / values.size
        (brand,average)
    })


    val reg3 = reg.count()

    (reg1, reg2, reg3)
  }

  override def doWorkload(df: Option[DataFrame] = None, spark: SparkSession): DataFrame = {
    val timestamp = System.currentTimeMillis()
    val (t, pi) = time(execution(spark))
    spark.createDataFrame(Seq(ReviewResult("review", timestamp, t, pi)))
  }
}
