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
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.math.random


case class SocialMediaResult(
                          name: String,
                          timestamp: Long,
                          total_runtime: Long,
                          pi_approximate: Double
                        )

object SocialMedia extends WorkloadDefaults {
  val name = "social-media"
  def apply(m: Map[String, Any]): SocialMedia =
    new SocialMedia(input = m.get("input").map(_.asInstanceOf[String]),
      output = None,
      slices = getOrDefault[Int](m, "slices", 2)
    )
}

case class SocialMedia(input: Option[String] = None,
                   output: Option[String] = None,
                   saveMode: String = SaveModes.error,
                   slices: Int
                  ) extends Workload {

  private def execution(spark: SparkSession): Double = {
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = spark.sparkContext.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if ((x * x) + (y * y) <= 1) 1 else 0
    }.reduce(_ + _)
    val piApproximate = 4.0 * count / (n - 1)
    piApproximate
  }

  override def doWorkload(df: Option[DataFrame] = None, spark: SparkSession): DataFrame = {
    val timestamp = System.currentTimeMillis()
    val (t, pi) = time(execution(spark))
    spark.createDataFrame(Seq(SocialMediaResult("sparkpi", timestamp, t, pi)))
  }
}
