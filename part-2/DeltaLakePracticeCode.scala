// Databricks notebook source
// MAGIC %md
// MAGIC ## Import Data and Create the temp table

// COMMAND ----------

//Configure location of dummy data in parquet format
val us_data_path = "/FileStore/tables/us_state_dummy_data/dummy_data_snappy.parquet"

// Read the dummy data with us_state and dummy count
val us_state_wise_count  = spark.read.parquet(us_data_path)

// Create table
us_state_wise_count.createOrReplaceTempView("us_state_wise_count")

// Display the sample data
display(spark.sql("select * from us_state_wise_count"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Create the Delta Lake Table

// COMMAND ----------

// MAGIC %sql 
// MAGIC DROP TABLE IF EXISTS count_by_state_delta;
// MAGIC  
// MAGIC CREATE TABLE count_by_state_delta
// MAGIC USING delta
// MAGIC LOCATION '/count_by_state_delta'
// MAGIC AS SELECT * FROM us_state_wise_count;
// MAGIC  
// MAGIC SELECT * FROM count_by_state_delta

// COMMAND ----------

// MAGIC %md
// MAGIC ## Explore the files created under the delta table path

// COMMAND ----------

// MAGIC %fs
// MAGIC ls /count_by_state_delta

// COMMAND ----------

// MAGIC %fs
// MAGIC ls /count_by_state_delta/_delta_log

// COMMAND ----------

// MAGIC %md
// MAGIC ## Lets explore how Delta Lake deal with multiple concurrent reads and writes

// COMMAND ----------

val state_readStream = spark.readStream.format("delta").load("/count_by_state_delta")
state_readStream.createOrReplaceTempView("state_readStream")

// COMMAND ----------

// MAGIC %sql
// MAGIC select state, sum(`count`) as dummyCount from state_readStream group by state

// COMMAND ----------

// MAGIC %md
// MAGIC ### Lets create dummy stream data and load into delta table

// COMMAND ----------

import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.functions._
import scala.util.Random

def generate_dummy_stream(deltaTablePath:String,checkpointPath:String,streamName:String)
{
  val stateGen = () => {
    val rand = new Random()
    val gen = List("CA", "WA","IA")
    gen.apply(rand.nextInt(3))
  }

  def random_dir(): String = {
    val r = new Random()
    checkpointPath+"chk/chk_pointing_" + r.nextInt(10000)
  }

  val sch = StructType(List(
    StructField("addr_state", DataTypes.StringType),
    StructField("count", DataTypes.LongType),
    StructField("sum", DataTypes.IntegerType)
  ))

  val df = spark.readStream.format("rate").option("rowsPerSecond", 1).load()

  val state_gen = spark.udf.register("stateGen", stateGen)

  val new_df = df.withColumn("state", state_gen())
    .withColumn("count", lit(1))
    .select("state", "count")


  new_df.writeStream
    .queryName(streamName)
    .trigger(Trigger.ProcessingTime("5 seconds"))
    .format("delta")
    .option("path", deltaTablePath)
    .option("checkpointLocation", random_dir())
    .start()
}

// COMMAND ----------

// MAGIC %md
// MAGIC #### First Stream to load delta table

// COMMAND ----------

generate_dummy_stream("/count_by_state_delta","/checkpoint_count_by_state_delta","first stream")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Second Stream to load delta table

// COMMAND ----------

generate_dummy_stream("/count_by_state_delta","/checkpoint2_count_by_state_delta","second stream")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Let read on the same delta table batch query

// COMMAND ----------

val state_readBatch = spark.read.format("delta").load("/count_by_state_delta")
state_readBatch.createOrReplaceTempView("state_readBatch")

// COMMAND ----------

// MAGIC %sql
// MAGIC select state, sum(`count`) as dummyCount from state_readBatch group by state

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Stop all Stream queries

// COMMAND ----------

 spark.streams.active.foreach(s => {
      if(s.isActive)
        s.stop()
    })

// COMMAND ----------


