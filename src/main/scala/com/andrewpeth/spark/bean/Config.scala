package com.andrewpeth.spark.bean

case class Config(pipelineType : String = "",
                  inputFormat : String = "",
                  outputFormat : String = "",
                  inputLocation : String = "",
                  outputLocation : String = "",
                  fileMode : String = "",
                  rootTag : String = "",
                  rowTag : String = "",
                  queryable : Boolean = false,
                  dbType : String = "")

