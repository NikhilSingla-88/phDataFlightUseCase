# Databricks notebook source
# MAGIC %fs rm dbfs:/FileStore/shared_uploads/ph_data_assignment/sfConfig.config

# COMMAND ----------

dbutils.fs.cp("FileStore/shared_uploads/ph_data_assignment/sfConfig.config", "file:///tmp/sfConfig.config")

# COMMAND ----------

## read all the credentials using config file
from configparser import ConfigParser

config_object = ConfigParser()
config_object.read("/tmp/sfConfig.config")

sfInfo = config_object["sfInfo"]
print(sfInfo["url"])
sfOptions = {
  "sfUrl" : sfInfo["url"],
  "sfUser" : sfInfo["sfUser"],
  "sfPassword" : sfInfo["password"],
  "sfDatabase" : sfInfo["database"],
  "sfSchema" : sfInfo["schema"],
  "sfWarehouse" : sfInfo["warehouse"]
}

# COMMAND ----------

# MAGIC %fs rm dbfs:/FileStore/shared_uploads/ph_data_assignment/flights/partition_08-1.csv

# COMMAND ----------

df_airlines=spark.read.format("csv").options(header=True,inferSchema=True).load("dbfs:/FileStore/shared_uploads/ph_data_assignment/airlines.csv")
df_airlines.cache()
#df_airlines.show(10,False)

# COMMAND ----------

df_airports=spark.read.format("csv").options(header=True,inferSchema=True).load("dbfs:/FileStore/shared_uploads/ph_data_assignment/airports.csv")
df_airports.cache()
#df_airports.show(10,False)

# COMMAND ----------

df_flights=spark.read.format("csv").options(header=True,inferSchema=True).load("dbfs:/FileStore/shared_uploads/ph_data_assignment/flights/*.csv")
#df_flights.show(10,False)
df_flights.cache()

# COMMAND ----------

#Total number of flights by airline and airport on a monthly basis
#Find the origin/destination airport and airlines, count the flights then
from pyspark.sql.functions import *
df_dest_flight=df_flights.select("year","month",col("DESTINATION_AIRPORT").alias("airport_code"),col("AIRLINE").alias("airline_code"))
df_origin_flight=df_flights.select("year","month",col("ORIGIN_AIRPORT").alias("airport_code"),col("AIRLINE").alias("airline_code"))
df_tot_flight=df_dest_flight.unionAll(df_origin_flight)
df_tot_flight_tmp=df_tot_flight.groupBy("year","month","airport_code","airline_code").agg(count("airport_code").alias("total"))
df_tot_flight=df_tot_flight_tmp.alias('a').join(df_airlines.alias('b')).where("a.airline_code=b.IATA_CODE").join(df_airports.alias('c')).where("a.airport_code=c.IATA_CODE").orderBy("year","month","AIRLINE","AIRPORT","total").select("year","month","AIRLINE","AIRPORT","total")
#print("total no of records - ",df_tot_flight.count())
display(df_tot_flight)


# COMMAND ----------

#On-time percentage of each airline for the year 2015
df_yr_data=df_flights.select("airline").where("year='2015'")
df_tot_data=df_yr_data.groupBy("airline").agg(count("airline").alias("tot_cnt"))
df_delay_data=df_yr_data.where("NVL(ARRIVAL_DELAY,0)<>0  OR NVL(DEPARTURE_DELAY,0)<>0").groupBy("airline").agg(count("airline").alias("delay_cnt"))
df_pre_final=df_tot_data.alias("a").join(df_delay_data.alias("b"),"airline").selectExpr("airline as airline_code","round(((tot_cnt-delay_cnt)*100)/(tot_cnt),3) as on_time_perc")
df_on_time=df_pre_final.join(df_airlines,df_pre_final.airline_code==df_airlines.IATA_CODE,"inner").select("airline","on_time_perc").orderBy("airline")
display(df_on_time)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
#Airlines with the largest number of delays
#For airline with max delay. Delay canbe +/- so excluded with 0
df_pre_final=df_flights.where("NVL(ARRIVAL_DELAY,0)=0").select(col("airline").alias("airline_code")).groupBy("airline_code").agg(count("airline_code").alias("cnt"))
wind_fun=Window.orderBy(col("cnt").desc())
df_largest_delay_airline=df_pre_final.withColumn("rn",row_number().over(wind_fun)).where("rn=1").join(df_airlines,df_airlines.IATA_CODE==df_pre_final.airline_code).selectExpr("airline","cnt")
display(df_largest_delay_airline.show())

# COMMAND ----------

#Cancellation reasons by airport
from pyspark.sql.functions import when,coalesce
df_canc=df_flights.where("CANCELLED=1")
df_src_canc=df_canc.selectExpr("CANCELLATION_REASON","ORIGIN_AIRPORT as airport_cd")
df_dest_canc=df_canc.selectExpr("CANCELLATION_REASON","DESTINATION_AIRPORT as airport_cd")
df_union_canc=df_src_canc.unionAll(df_dest_canc).distinct()
df_derive_canc_reason=df_union_canc.withColumn("CANCELLATION_REASON",when(df_union_canc.CANCELLATION_REASON=='A','Airline/Carrier')
.when(df_union_canc.CANCELLATION_REASON=='B','Weather')
.when(df_union_canc.CANCELLATION_REASON=='C','National Air System')
.when(df_union_canc.CANCELLATION_REASON=='D','Security')
.otherwise( df_union_canc.CANCELLATION_REASON)                                                                 ).join(df_airports,df_airports.IATA_CODE==df_union_canc.airport_cd).select("airport","CANCELLATION_REASON").orderBy("airport")
display(df_derive_canc_reason)

# COMMAND ----------

#Delay reasons by airport
from pyspark.sql.functions import when,coalesce,lit
df_delay_rsn=df_flights.withColumn("type_of_delay",when(coalesce(df_flights.AIR_SYSTEM_DELAY,lit('0'))!=0,'AIR_SYSTEM_DELAY')
                                  .when(coalesce(df_flights.SECURITY_DELAY,lit('0'))!=0,'SECURITY_DELAY')
                                  .when(coalesce(df_flights.AIRLINE_DELAY,lit('0'))!=0,'AIRLINE_DELAY')
                                  .when(coalesce(df_flights.LATE_AIRCRAFT_DELAY,lit('0'))!=0,'LATE_AIRCRAFT_DELAY')
                                  .when(coalesce(df_flights.WEATHER_DELAY,lit('0'))!=0,'WEATHER_DELAY')
                                   .otherwise('NO_DELAY')
                                  ).select("ORIGIN_AIRPORT","DESTINATION_AIRPORT","type_of_delay")

df_src_delay_rsn=df_delay_rsn.selectExpr ("ORIGIN_AIRPORT as airport_code","type_of_delay")
df_dest_delay_rsn=df_delay_rsn.selectExpr("DESTINATION_AIRPORT as airport_code","type_of_delay")
df_delay_rsn=df_src_delay_rsn.unionAll(df_dest_delay_rsn).distinct()
df_final_delay_rsn=df_delay_rsn.join(df_airports,df_delay_rsn.airport_code==df_airports.IATA_CODE).select("airport","type_of_delay")
display(df_final_delay_rsn)


# COMMAND ----------

#Airline with the most unique routes
df_rout1=df_flights.selectExpr("ORIGIN_AIRPORT as src","DESTINATION_AIRPORT as dest","airline as airline_cd")
#df_rout1
df_rout2=df_flights.selectExpr("DESTINATION_AIRPORT as src","ORIGIN_AIRPORT as dest","airline as airline_cd")
#df_rout2
df_all_routes=df_rout1.unionAll(df_rout2).groupBy("src","dest","airline_cd").agg(count("src").alias("cnt")).orderBy("cnt","airline_cd")
df_final_uniq_routes=df_all_routes.join(df_airlines,df_all_routes.airline_cd==df_airlines.IATA_CODE).select("airline","cnt").distinct().orderBy("cnt","Airline")
display(df_final_uniq_routes)


# COMMAND ----------


#Airline with the most unique routes
df_final_uniq_routes.write.format("snowflake").options(**sfOptions).option("dbtable","most_uniq_routes_aline").mode("Overwrite").save()

# COMMAND ----------

#testing to ingestion
df_airlines.write.format("snowflake").options(**sfOptions).option("dbtable","airlines").mode("Overwrite").save()

# COMMAND ----------



# COMMAND ----------


#Total number of flights by airline and airport on a monthly basis
df_tot_flight.write.format("snowflake").options(**sfOptions).option("dbtable","tot_no_flgt_aline_aport").mode("Overwrite").save()
#On-time percentage of each airline for the year 2015
df_on_time.write.format("snowflake").options(**sfOptions).option("dbtable","on_time_perc_aline").mode("Overwrite").save()
df_largest_delay_airline.write.format("snowflake").options(**sfOptions).option("dbtable","larg_delay_aline").mode("Overwrite").save()
#Cancellation reasons by airport
df_derive_canc_reason.write.format("snowflake").options(**sfOptions).option("dbtable","canc_reason_aport").mode("Overwrite").save()
#Delay reasons by airport 
df_final_delay_rsn.write.format("snowflake").options(**sfOptions).option("dbtable","delay_reason_aport").mode("Overwrite").save()

# COMMAND ----------

df_tot_no_flgt_aline_aport = spark.read \
  .format("snowflake") \
  .options(**sfOptions) \
  .option("dbtable", "tot_no_flgt_aline_aport") \
  .load()
display(df_tot_no_flgt_aline_aport)

# COMMAND ----------

df_on_time_perc_aline = spark.read \
  .format("snowflake") \
  .options(**sfOptions) \
  .option("dbtable", "on_time_perc_aline") \
  .load()
display(df_on_time_perc_aline)

# COMMAND ----------

df_larg_delay_aline = spark.read \
  .format("snowflake") \
  .options(**sfOptions) \
  .option("dbtable", "larg_delay_aline") \
  .load()
display(df_larg_delay_aline)

# COMMAND ----------

df_canc_reason_aport = spark.read \
  .format("snowflake") \
  .options(**sfOptions) \
  .option("dbtable", "canc_reason_aport") \
  .load()
display(df_canc_reason_aport)

# COMMAND ----------

df_delay_reason_aport = spark.read \
  .format("snowflake") \
  .options(**sfOptions) \
  .option("dbtable", "delay_reason_aport") \
  .load()

display(df_delay_reason_aport)

# COMMAND ----------

df_most_uniq_routes_aline = spark.read \
  .format("snowflake") \
  .options(**sfOptions) \
  .option("dbtable", "most_uniq_routes_aline") \
  .load()
display(df_most_uniq_routes_aline)
