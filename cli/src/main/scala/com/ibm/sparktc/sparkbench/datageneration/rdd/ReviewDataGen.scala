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

package com.ibm.sparktc.sparkbench.datageneration.rdd

import com.ibm.sparktc.sparkbench.workload.ml.KMeansWorkload
import com.ibm.sparktc.sparkbench.utils.SparkFuncs.writeToDisk
import com.ibm.sparktc.sparkbench.workload.{Workload, WorkloadDefaults}
import com.ibm.sparktc.sparkbench.utils.GeneralFunctions._
import com.ibm.sparktc.sparkbench.utils.SaveModes
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

object ReviewDataGen extends WorkloadDefaults {
  val name = "amazon-review-gendata"
  override def apply(m: Map[String, Any]): ReviewDataGen = new ReviewDataGen(
    input = m.get("input").map(_.asInstanceOf[String]),
    output = Some(getOrThrow(m, "output").asInstanceOf[String]),
    saveMode = getOrDefault[String](m, "save-mode", SaveModes.error),
    scaling = getOrDefault[Double](m, "scaling", 0.1),
    numPartitions = getOrDefault[Int](m, "partitions", -1)
  )
}

case class ReviewDataGen(input: Option[String] = None,
                         output: Option[String],
                         saveMode: String,
                         scaling: Double,
                         numPartitions: Int) extends Workload {

  override def doWorkload(df: Option[DataFrame] = None, spark: SparkSession): DataFrame = {
    val timestamp = System.currentTimeMillis()

    val (loadTime, input_df) = time {
      spark.read.json(input.get)
    }
    val (generateTime, dataDF) = time {
      input_df.sample(scaling)
    }

    val (saveTime, _) = time { writeToDisk(output.get, saveMode, dataDF, spark) }

    val timeResultSchema = StructType(
      List(
        StructField("name", StringType, nullable = false),
        StructField("timestamp", LongType, nullable = false),
        StructField("loading", LongType, nullable = true),
        StructField("generate", LongType, nullable = true),
        StructField("save", LongType, nullable = true),
        StructField("total_runtime", LongType, nullable = false)
      )
    )

    val total = loadTime + generateTime + saveTime

    val timeList = spark.sparkContext.parallelize(Seq(Row("reviews", timestamp,loadTime, generateTime, saveTime, total)))

    spark.createDataFrame(timeList, timeResultSchema)
  }
}
