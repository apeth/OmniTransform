package com.andrewpeth.spark.model

case class DatabaseProperties(url : String,
                              driver: String,
                              dbtable : String,
                              user : String,
                              var password : Array[Char]) {

}
