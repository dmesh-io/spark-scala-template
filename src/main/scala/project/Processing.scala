/*
 * Copyright 2017 Daniel Bast
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package project

import buildinfo.BuildInfo
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{ Dataset, Row, SparkSession }
import org.rogach.scallop._

import scala.Option

class Processing(spark: SparkSession) extends LazyLogging {

  val steps = new Steps(spark)

  def process(
      input: String,
      output: String,
      lines: Option[Int],
      filterOpt: Option[String],
      debug: Boolean
  ): Unit = {
    logger.info("Starting processing")

    val rawData = steps.read(input)
    if (debug) {
      logger.debug(s"The schema is now: ${rawData.schema.treeString}")
    }

    val processed = filterOpt.fold(ifEmpty = rawData)(filterValue => filter(rawData, filterValue))

    steps.writeOrShowData(processed, output, linesToShow = lines)
    logger.info("Finished processing")
  }

  def filter(
      df: Dataset[Row],
      filter: String
  ): Dataset[Row] = {
    logger.info(s"Filtering data based on: $filter")
    df.filter(filter)
  }
}

/**
  * Main cli parsing class
  *
  * @param arguments The unparsed command line arguments
  */
class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  appendDefaultToDescription = true
  val nodes = opt[String](descr = "Spark nodes (local run)", default = Option("local[*]"))
  val stay = opt[Boolean](
    descr =
      "Wait for key press to exit (to keep SparkSession and webserver running while debugging)",
    default = Option(false)
  )

  val input = opt[String](
    descr = "Path to the raw data to process (local, hdfs, s3)",
    required = true
  )
  val output = opt[String](descr = "Output path (local, hdfs, s3)", required = true)
  val limit =
    opt[Int](
      descr = "Limit DAG steps to given number, the read and write/show steps are always added"
    )
  val linesToShow = opt[Int](
    descr =
      "Amount of lines to shows to the console (instead of writing snappy compressed parquet files)"
  )
  val debug = opt[Boolean](
    descr = "Explains plan during DAG construction",
    default = Option(false)
  )

  val sqlFilter = opt[String](
    descr = "conditionExpr to be applied on a filter .filter('conditionExpr')",
    required = false
  )

  verify()
}

object Processing extends LazyLogging {
  def main(args: Array[String]): Unit = {
    logger.info(s"Starting '${BuildInfo.name}' version '${BuildInfo.version}'")

    val conf = new Conf(args)
    logger.info(s"The command line parameters are: ${conf.summary}")

    lazy val spark = SparkSession.builder
      .master(conf.nodes())
      .appName(BuildInfo.name)
      .getOrCreate()

    val processing = new Processing(spark)
    try processing.process(
      input = conf.input(),
      output = conf.output(),
      lines = conf.linesToShow.toOption,
      filterOpt = conf.sqlFilter.toOption,
      debug = conf.debug()
    )
    finally {
      if (conf.stay()) {
        logger.info("Waiting: press enter to exit")
        logger.info(System.in.read().toString)
      }
      spark.stop()
    }
  }
}
