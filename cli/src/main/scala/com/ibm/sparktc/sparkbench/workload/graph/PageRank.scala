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

package com.ibm.sparktc.sparkbench.workload.graph

import com.ibm.sparktc.sparkbench.utils.GeneralFunctions.{getOrDefault, time}
import com.ibm.sparktc.sparkbench.utils.SaveModes
import com.ibm.sparktc.sparkbench.workload.{Workload, WorkloadDefaults}
import org.apache.spark.sql.{DataFrame, SparkSession}

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.storage.StorageLevel

case class PageRankResult(
                         name: String,
                         timestamp: Long,
                         total_runtime: Long,
                         pi_approximate: Long
                       )

object PageRank extends WorkloadDefaults {
  val name = "pagerank"
  def apply(m: Map[String, Any]): Workload = {
    val input = m.get("followers").map(_.asInstanceOf[String])
    val output = None
    val user = m.get("users").map(_.asInstanceOf[String])
    val maxiterations = getOrDefault[Int](m, "maxiterations", 2)
    val resetProb = getOrDefault[Double](m, "resetProb", 0.15)
    val cachePolicy = m.get("cache").map(_.asInstanceOf[String])

    cachePolicy.get match {
      case "All" =>  {
        println("##################  All ###############################\n")
        PageRank_All(input=input, output = output, user = user,
          maxiterations = maxiterations, resetProb = resetProb, cachePolicy = cachePolicy)
      }
      case "SODA" => {
        println("##################  SODA ###############################\n")
        PageRank_SODA(input=input, output = output, user = user,
          maxiterations = maxiterations, resetProb = resetProb, cachePolicy = cachePolicy)
      }
      case "None" => {
        println("##################  None ###############################\n")
        PageRank_None(input=input, output = output, user = user,
          maxiterations = maxiterations, resetProb = resetProb, cachePolicy = cachePolicy)
      }
      case _ => {
        PageRank_None(input=input, output = output, user = user,
          maxiterations = maxiterations, resetProb = resetProb, cachePolicy = cachePolicy)
      }
    }
  }
}

case class PageRank_All(input: Option[String] = None,
                    output: Option[String] = None,
                    user: Option[String] = None,
                    saveMode: String = SaveModes.error,
                    maxiterations: Int,
                    resetProb: Double,
                    cachePolicy: Option[String]
                   ) extends Workload {

  private def execution(spark: SparkSession): Long = {

    val graph = GraphLoader.edgeListFile(sc = spark.sparkContext,
      path = input.get,
      canonicalOrientation = true,
      numEdgePartitions = -1,
      edgeStorageLevel = StorageLevel.MEMORY_ONLY,
      vertexStorageLevel = StorageLevel.MEMORY_ONLY)
    // Run PageRank
    val ranks  = graph.staticPageRank(maxiterations, resetProb).vertices

    ranks.count()
  }

  override def doWorkload(df: Option[DataFrame] = None, spark: SparkSession): DataFrame = {
    val timestamp = System.currentTimeMillis()
    val (t, pi) = time(execution(spark))
    spark.createDataFrame(Seq(PageRankResult("pagerank", timestamp, t, pi)))
  }
}

case class PageRank_SODA(input: Option[String] = None,
                    output: Option[String] = None,
                    user: Option[String] = None,
                    saveMode: String = SaveModes.error,
                    maxiterations: Int,
                    resetProb: Double,
                    cachePolicy: Option[String]
                   ) extends Workload {

  private def execution(spark: SparkSession): Long = {

    val graph = GraphLoader.edgeListFile(sc = spark.sparkContext,
      path = input.get,
      canonicalOrientation = true,
      numEdgePartitions = -1,
      edgeStorageLevel = StorageLevel.MEMORY_ONLY,
      vertexStorageLevel = StorageLevel.DISK_ONLY)
    // Run PageRank
    val ranks  = graph.staticPageRank(maxiterations, resetProb).vertices

    ranks.count()
  }

  override def doWorkload(df: Option[DataFrame] = None, spark: SparkSession): DataFrame = {
    val timestamp = System.currentTimeMillis()
    val (t, pi) = time(execution(spark))
    spark.createDataFrame(Seq(PageRankResult("pagerank", timestamp, t, pi)))
  }
}


case class PageRank_None(input: Option[String] = None,
                    output: Option[String] = None,
                    user: Option[String] = None,
                    saveMode: String = SaveModes.error,
                    maxiterations: Int,
                    resetProb: Double,
                    cachePolicy: Option[String]
                   ) extends Workload {

  private def execution(spark: SparkSession): Long = {

    val graph = GraphLoader.edgeListFile(sc = spark.sparkContext,
      path = input.get,
      canonicalOrientation = true,
      numEdgePartitions = -1,
      edgeStorageLevel = StorageLevel.DISK_ONLY,
      vertexStorageLevel = StorageLevel.DISK_ONLY)
    // Run PageRank
    val ranks  = graph.staticPageRank(maxiterations, resetProb).vertices

    ranks.count()
  }

  override def doWorkload(df: Option[DataFrame] = None, spark: SparkSession): DataFrame = {
    val timestamp = System.currentTimeMillis()
    val (t, pi) = time(execution(spark))
    spark.createDataFrame(Seq(PageRankResult("pagerank", timestamp, t, pi)))
  }
}

