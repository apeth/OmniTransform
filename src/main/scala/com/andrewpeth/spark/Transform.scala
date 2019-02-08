package com.andrewpeth.spark

import com.andrewpeth.spark.pipelines._
import org.apache.spark._
import org.apache.spark.sql._
import com.andrewpeth.spark.constants.Constant
import com.andrewpeth.spark.utility.CLIParser
import org.apache.log4j.Logger
import org.apache.log4j.Level
import com.andrewpeth.spark.bean.Config


/**
  *
  * @param spark
  * @param config
  */
class Transform(@transient spark : SparkSession, config : Config) extends Serializable {
  def execute(): Unit = {
    val pipeline = createPipeline(config)
    pipeline.start()
  }

  def createPipeline(config: Config): Pipeline = {
    config.pipelineType.toUpperCase match {
      case Constant.FILE_PIPELINE => new FilePipeline(spark, config)
      case Constant.KAFKA_PIPELINE => new KafkaPipeline(spark, config)
      case Constant.HIVE_PIPELINE => new HivePipeline(spark, config)
      case Constant.IMPALA_PIPELINE => new ImpalaPipeline(spark, config)
      case Constant.DATABASE_PIPELINE => new DatabasePipeline(spark, config)
      case _ =>
        print("Invalid pipeline type defined. Must be: FILE_PIPELINE, KAFKA_PIPELINE, HIVE_PIPELINE, IMPALA_PIPELINE, or DATABASE_PIPELINE.")
        throw new RuntimeException("Invalid pipeline type selected.")
    }
  }
}

/**
  * Program entrance point - Creates and executes a transformation pipeline.
  */
object Transform {
  def main(args: Array[String]): Unit = {
    val config = CLIParser.parseCommandLineArguments(args)
    print(
      s"""Configuration Used:
            Pipeline Type: ${config.pipelineType}
            Input Format: ${config.inputFormat}
            Input Location: ${config.inputLocation}
            Output Format: ${config.outputFormat}
            Output Location: ${config.outputLocation}
            File Mode: ${config.fileMode}
            XML Root Tag (Optional): ${config.rootTag}
            XML Row Tag (Optional): ${config.rowTag}
        """.stripMargin)

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val spark: SparkSession = SparkSession
      .builder
      .appName("SparkTransformationPipeline")
      .config("spark.master", "local[*]") //Remove when not running locally.
      .getOrCreate()


    val pipeline = new Transform(spark, config)
    pipeline.execute()
  }
}
