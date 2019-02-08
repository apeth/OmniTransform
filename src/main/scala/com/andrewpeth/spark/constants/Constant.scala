package com.andrewpeth.spark.constants

object Constant {
  //***** File Types ******
  final lazy val JSON = "JSON"
  final lazy val CSV = "CSV"
  final lazy val XML = "XML"
  final lazy val AVRO = "AVRO"
  final lazy val PARQUET = "PARQUET"
  final lazy val TEXT = "TEXT"
  final lazy val GLOB = "*."
  final lazy val GLOB_ALL = "*"
  final lazy val ORC = "ORC"

  //****Pipeline Types****
  final lazy val FILE_PIPELINE = "FILE_PIPELINE"
  final lazy val KAFKA_PIPELINE = "KAFKA_PIPELINE"
  final lazy val HIVE_PIPELINE = "HIVE_PIPELINE"
  final lazy val IMPALA_PIPELINE = "IMPALA_PIPELINE"
  final lazy val DATABASE_PIPELINE = "DATABASE_PIPELINE"

  //****String Constants****
}
