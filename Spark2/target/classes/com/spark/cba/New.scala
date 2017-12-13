package com.spark.cba

import org.apache.spark.sql.functions.{col,split}

object Main{
  def main(args:Array[String])
  {
   // System.setProperty("hadoop.home.dir","G:/Hadoop_SW/Winutils")  // this is to write it in local
    val sparkConf= new org.apache.spark.SparkConf()
    sparkConf.setAppName("Test").setMaster("local")
    
    //Below line is to created alias name for all coulmns
    
    val columnWithAlias = List(("_c0","City"),("_c1","Latitude"),("_c2","Timestamp"),("_c3","Conditions"),("_c4","Temperature"),
        ("_c5","Pressure"),("_c6","Humidity"))
    
    val renamedColumns = columnWithAlias.map(x => (col(x._1).as(x._2)))
    
    val sparkcontext=new org.apache.spark.SparkContext(sparkConf)
    val sqlcontext =new org.apache.spark.sql.SQLContext(sparkcontext)
    //Mentioned local path below
    val dataframe = sqlcontext.read.format("csv").option("header", "false").option("delimiter","|").
    load("/Spark/src/Climatedata")
    
    val df2=dataframe.select(renamedColumns : _*) //it will get all renamed column names
    
     import sqlcontext.implicits._  //t ouse native rdd functions
    
    val resdf = df2.select($"City",$"Latitude",split($"Timestamp"," ")(0).alias("Date"),
      split($"Timestamp"," ")(1).alias("Time"),$"Conditions",$"Temperature",$"Pressure",$"Humidity"
        )
        
    resdf.createOrReplaceTempView("climatedata")
    
    val table = sqlcontext.sql("select *,Rank() Over(order by Humidity desc) as Rank from climatedata")
    table.show()   
  }
}