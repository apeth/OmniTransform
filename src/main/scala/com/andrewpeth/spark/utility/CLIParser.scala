package com.andrewpeth.spark.utility

import com.andrewpeth.spark.bean.Config

object CLIParser {
  def parseCommandLineArguments(args: Array[String]): Config = {
    var pipelineType = ""
    var inputFormat = ""
    var outputFormat = ""
    var inputLocation = ""
    var outputLocation = ""
    var fileMode = ""
    var rootTag = ""
    var rowTag = ""
    var query = "false"
    var dbType = ""

   args.sliding(2, 1).toList.collect {
      case Array("--pipeline", pipeline: String) => pipelineType = pipeline
      case Array("--input-format", iFormat: String) => inputFormat = iFormat
      case Array("--output-format", oFormat: String) => outputFormat = oFormat
      case Array("--input-location", iLocation: String) => inputLocation = iLocation
      case Array("--output-location", oLocation: String) => outputLocation = oLocation
      case Array("--file-mode", fMode: String) => fileMode = fMode
      case Array("--root-tag", root: String) => rootTag = root
      case Array("--row-tag", row: String) => rowTag = row
      case Array("--query-runtime", q: String) => query = q
      case Array("--database-type", db: String) => dbType = db
    }
    Config(pipelineType, inputFormat, outputFormat, inputLocation, outputLocation, fileMode, rootTag, rowTag, query.toBoolean, dbType)
  }
}
