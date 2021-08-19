// Databricks notebook source
// DBTITLE 1,Use sample covid data and create two DataFrames "FullData" and "SmallData"
val fullData = spark.read.option("header","true")
  .csv("/FileStore/tables/covid_data/us_states_covid19_daily.csv")
  .select("date","state","negative","positive","recovered","totalTestResults")

fullData.write.format("delta").mode("overwrite").save("/FileStore/tables/covid_data/full_data")

val smallData = fullData.limit(10)
smallData.write.format("delta").mode("overwrite").save("/FileStore/tables/covid_data/small_data")


// COMMAND ----------

// MAGIC %md 
// MAGIC ### Create the delta tables
// MAGIC first create the table for the full data as full_tbl
// MAGIC 
// MAGIC second create the table for the small data as small_tbl

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC drop table if exists full_tbl;
// MAGIC drop table if exists small_tbl;
// MAGIC 
// MAGIC create table full_tbl using delta location '/FileStore/tables/covid_data/full_data';
// MAGIC create table small_tbl using delta location '/FileStore/tables/covid_data/small_data';

// COMMAND ----------

// MAGIC %md
// MAGIC ###Querying the small table and see the result

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from small_tbl 

// COMMAND ----------

// MAGIC %md
// MAGIC ### Lets create the first stream from the small table

// COMMAND ----------

val smallStream = spark.readStream.format("delta").load("/FileStore/tables/covid_data/small_data")

// COMMAND ----------

// MAGIC %md
// MAGIC ### write the stream data into stream delta table
// MAGIC It will write the first 10 row 

// COMMAND ----------

import org.apache.spark.sql.streaming.{OutputMode, Trigger}

smallStream.writeStream
    .trigger(Trigger.Once())
    .format("delta")
    .option("path", "/FileStore/tables/covid_data/small_stream")
    .option("checkpointLocation","/FileStore/tables/covid_data/small_stream/_checkpoint")
    .start()

// COMMAND ----------

// MAGIC %md
// MAGIC ### lets insert more data to small table
// MAGIC Starting of the notebook we have created two tables full and small
// MAGIC 
// MAGIC By using full table load random 10 more rows to small table

// COMMAND ----------

// MAGIC %sql
// MAGIC insert into small_tbl
// MAGIC select * from full_tbl order by rand() limit 10

// COMMAND ----------

// MAGIC %md
// MAGIC ### write more data into stream delta table
// MAGIC It will write 10 more rows to stream table 

// COMMAND ----------

import org.apache.spark.sql.streaming.{OutputMode, Trigger}

smallStream.writeStream
    .trigger(Trigger.Once())
    .format("delta")
    .option("path", "/FileStore/tables/covid_data/small_stream")
    .option("checkpointLocation","/FileStore/tables/covid_data/small_stream/_checkpoint")
    .start()


// COMMAND ----------

// MAGIC %md
// MAGIC ###Our First Update Operation
// MAGIC lets update the few rows

// COMMAND ----------

// MAGIC %sql
// MAGIC update small_tbl set recovered = '0' where state = 'AK'

// COMMAND ----------

// MAGIC %md
// MAGIC ### write updated data to stream delta table
// MAGIC #### But it will throw an exception  

// COMMAND ----------

import org.apache.spark.sql.streaming.{OutputMode, Trigger}

smallStream.writeStream
    .trigger(Trigger.Once())
    .format("delta")
    .option("path", "/FileStore/tables/covid_data/small_stream")
    .option("checkpointLocation","/FileStore/tables/covid_data/small_stream/_checkpoint")
    .start()


// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Delete some rows from small table

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC delete from small_tbl table where state = 'CA'

// COMMAND ----------

// MAGIC %md
// MAGIC ### Change stream delta table for deleted rows
// MAGIC #### But it will throw an exception  

// COMMAND ----------

import org.apache.spark.sql.streaming.{OutputMode, Trigger}

smallStream.writeStream
    .trigger(Trigger.Once())
    .format("delta")
    .option("path", "/FileStore/tables/covid_data/small_stream")
    .option("checkpointLocation","/FileStore/tables/covid_data/small_stream/_checkpoint")
    .start()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC 
// MAGIC ### Structured Streaming does not handle input that is not an append and throws an exception if any modifications occur on the table being used as a source. There are two main strategies for dealing with changes that cannot be automatically propagated downstream:
// MAGIC 
// MAGIC #### Ignore updates and deletes
// MAGIC You can delete the output and checkpoint and restart the stream from the beginning.
// MAGIC You can set either of these two options:
// MAGIC 
// MAGIC ignoreDeletes: ignore transactions that delete data at partition boundaries.
// MAGIC 
// MAGIC ignoreChanges: re-process updates if files had to be rewritten in the source table due to a data changing operation such as UPDATE, MERGE INTO, DELETE (within partitions), or OVERWRITE. Unchanged rows may still be emitted, therefore your **downstream consumers should be able to handle duplicates**. Deletes are not propagated downstream. ignoreChanges subsumes ignoreDeletes. Therefore if you use ignoreChanges, your stream will not be disrupted by either deletions or updates to the source table.

// COMMAND ----------

val smallStreamIgnoreUpdates = spark.readStream.format("delta").option("ignoreChanges","true").load("/FileStore/tables/covid_data/small_data")

// COMMAND ----------

// MAGIC %md
// MAGIC ### Now lets try to write small table updated and deleted data to stream table
// MAGIC 
// MAGIC small stream table holds duplicate data due to ignoreChanges option

// COMMAND ----------

import org.apache.spark.sql.streaming.{OutputMode, Trigger}

smallStreamIgnoreUpdates.writeStream
    .trigger(Trigger.Once())
    .format("delta")
    .option("path", "/FileStore/tables/covid_data/small_stream")
    .option("checkpointLocation","/FileStore/tables/covid_data/small_stream/_checkpoint")
    .start()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Explore the streaming data

// COMMAND ----------

val stream_df = spark.read.format("delta").load("/FileStore/tables/covid_data/small_stream")
display(stream_df)

// COMMAND ----------



// COMMAND ----------

display(small_df)

// COMMAND ----------


