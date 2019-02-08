package com.andrewpeth.spark.pipelines

import com.andrewpeth.spark.bean.Config
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import com.andrewpeth.spark.constants.Constant
import java.io.File
import com.databricks.spark.xml._
import com.databricks.spark.avro._
import scala.io.StdIn.readLine

/**
  *
  * @param spark
  * @param config
  */
class FilePipeline(@transient spark : SparkSession, config : Config) extends Pipeline {

  def start() : Unit = {
    val input = new File(config.inputLocation)
    val df : DataFrame = input match {
      case x if input.exists() && input.isDirectory =>
        printf("Input is a directory")
        readDirectory(config.inputFormat, config.inputLocation)
      case x if input.exists() && input.isFile =>
        printf("Input is a file")
        readFile(config.inputFormat, config.inputLocation)
      case _ => print("Input path is does not exist or is neither a file nor directory.")
        throw new RuntimeException
    }

    if (config.queryable) {
      val transformedDF = liveQueryRuntime(df)
      save(transformedDF, config.outputFormat, config.outputLocation)
    } else {
      save(df, config.outputFormat, config.outputLocation)
    }
  }

  def readDirectory(inputType: String, inputPath : String): DataFrame = {
    val df = inputType.toUpperCase match {
      case Constant.JSON => spark.read.option("multiline", true).json(inputPath + File.separator + Constant.GLOB_ALL)
      case Constant.CSV => spark.read.csv(inputPath + File.separator + Constant.GLOB_ALL)
      case Constant.AVRO => spark.read.format("com.databricks.spark.avro").load(inputPath + File.separator + Constant.GLOB_ALL)
      case Constant.PARQUET => spark.read.parquet(inputPath + File.separator + Constant.GLOB_ALL)
      case Constant.XML => spark.sqlContext.read.format("com.databricks.spark.xml").load(inputPath + File.separator + Constant.GLOB_ALL)
      case Constant.TEXT => spark.read.option("header", "false").csv(inputPath + File.separator + Constant.GLOB_ALL)
      case _ =>
        print("Input Type must be of type: JSON, CSV, AVRO, PARQUET, XML, or TEXT.")
        throw new IllegalArgumentException
    }
    df
  }

  def readFile(inputType: String, inputPath : String): DataFrame = {
    val df = inputType.toUpperCase match {
      case Constant.JSON => spark.read.option("multiline", true).json(inputPath)
      case Constant.CSV => spark.read.csv(inputPath)
      case Constant.AVRO => spark.read.format("com.databricks.spark.avro").load(inputPath)
      case Constant.PARQUET => spark.read.parquet(inputPath)
      case Constant.XML => spark.read.format("com.databricks.spark.xml").load(inputPath)
      case Constant.TEXT => spark.read.option("header", "false").csv(inputPath)
      case _ => {
        print("Input Type must be of type: JSON, CSV, AVRO, PARQUET, XML, or TEXT.")
        throw new IllegalArgumentException
      }
    }
    df
  }

  def save(df : DataFrame, outputFormat : String, outputLocation : String): Unit = {
    outputFormat match {
      case Constant.JSON => df.write.mode(SaveMode.Append).json(outputLocation)
      case Constant.CSV => df.write.mode(SaveMode.Append).csv(outputLocation)
      case Constant.AVRO => df.write.mode(SaveMode.Append).avro(outputLocation)
      case Constant.PARQUET => df.write.mode(SaveMode.Append).parquet(outputLocation)
      case Constant.TEXT => df.write.mode(SaveMode.Append).text(outputLocation)
      case Constant.XML => df.write.mode(SaveMode.Append).format("com.databricks.spark.xml")
        .option("rootTag", config.rootTag)
        .option("rowTag", config.rowTag)
        .save(outputLocation)
      case Constant.ORC => df.write.mode(SaveMode.Append).orc(outputLocation)
      case _ =>
        print("Output Type must be for type: JSON, CSV, AVRO, PARQUET, XML, TEXT or ORC.")
    }
  }

  /**
    * Query and transform your input in realtime, save output to configured format. Novelty item.
    * @param df Input datasource as dataframe
    * @return Transformed dataframe.
    */
  def liveQueryRuntime(df : DataFrame) : DataFrame = {
    println("Your Input Schema:")
    val inputSchema = df.printSchema().toString
    df.createOrReplaceTempView("MYTABLE")
    println("Your table's name is MYTABLE (Enter SELECT * FROM MYTABLE) or equivalent to proceed.\n")
    val input = readLine()
    val transformedDF = spark.sql(input.stripMargin)
    println("Your output schema:")
    transformedDF.printSchema()
    transformedDF.show(false)
    transformedDF
  }

}
