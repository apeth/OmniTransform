package com.andrewpeth.spark.pipelines

import java.io.FileInputStream
import java.util.Properties
import com.andrewpeth.spark.bean.Config
import com.andrewpeth.spark.model.DatabaseProperties
import org.apache.spark.sql.{DataFrame, SparkSession}

class DatabasePipeline(@transient spark : SparkSession, config : Config) extends Pipeline {
  def start(): Unit = {
    val dbProperties = jdbcUrlBuilder(config)
    val df = queryDatabase(dbProperties)
    df.printSchema()
  }

  def jdbcUrlBuilder(config : Config): DatabaseProperties = {
    val (url, port, table, user, password) =
      try {
        val prop = new Properties()
        prop.load(new FileInputStream("config.properties"))
        (
          prop.getProperty("database.host"),
          new Integer(prop.getProperty("database.port")),
          prop.getProperty("database.table"),
          prop.getProperty("database.user"),
          prop.getProperty("database.password").toCharArray
        )
      } catch { case e: Exception =>
        e.printStackTrace()
        sys.exit(1)
      }
    val driver = getDatabaseDriver(config)
    DatabaseProperties(url, driver, table, user, password)
  }

  def getDatabaseDriver(config : Config) : String = {
    val driver = config.dbType.toLowerCase() match  {
      case "postgresql" => "org.postgresql.Driver"
      case "mysql" => "com.mysql.jdbc.Driver"
      case _ => throw new RuntimeException
    }
    driver
  }

  def queryDatabase(dbProperties : DatabaseProperties): DataFrame = {
    val df = spark.read
        .format("jdbc")
        .option("url", dbProperties.url)
        .option("driver", dbProperties.driver)
        .option("dbtable", dbProperties.dbtable)
        .option("user", dbProperties.user)
        .option("password", dbProperties.password.toString)
        .load()

    df
  }
}
