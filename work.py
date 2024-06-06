import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import max, avg, sum, round
from datetime import datetime

from pyspark.sql.functions import unix_timestamp, to_timestamp, month
from pyspark.sql.functions import to_date, count, col, concat, lit
from graphframes import *

def fieldcleansing(dataframe):

  if (dataframe.first()['taxi_type']=='yellow_taxi'):
    columns = dataframe.columns
    dataframe = dataframe.select(*(col(c).cast("string").alias(c) for c in columns))
    dataframe = dataframe.filter(unix_timestamp(dataframe['tpep_pickup_datetime'],'yyyy-MM-dd HH:mm:ss').isNotNull())
    dataframe = dataframe.filter(unix_timestamp(dataframe['tpep_dropoff_datetime'],'yyyy-MM-dd HH:mm:ss').isNotNull())
    
    
  elif (dataframe.first()['taxi_type']=='green_taxi'):
    columns = dataframe.columns
    dataframe = dataframe.select(*(col(c).cast("string").alias(c) for c in columns))
    dataframe = dataframe.filter(unix_timestamp(dataframe['lpep_pickup_datetime'],'yyyy-MM-dd HH:mm:ss').isNotNull())
    dataframe = dataframe.filter(unix_timestamp(dataframe['lpep_dropoff_datetime'],'yyyy-MM-dd HH:mm:ss').isNotNull())
  
  
  dataframe = dataframe.filter((dataframe["trip_distance"] >= 0) & (dataframe["fare_amount"] >= 0))
  return dataframe 

#TASK 3
def filter(dataframe):
    return_dataframe = dataframe.select("tpep_pickup_datetime", "trip_distance", "fare_amount")
    return_dataframe = return_dataframe.withColumn("tpep_pickup_datetime",to_date(col("tpep_pickup_datetime")))
    return_dataframe = return_dataframe.filter((return_dataframe["tpep_pickup_datetime"] >= "2023-01-01") & (return_dataframe["tpep_pickup_datetime"] <= "2023-01-07") & ~((return_dataframe["trip_distance"] < 1) & (return_dataframe["fare_amount"] > 50)))
    return return_dataframe  

#TASK 5
def findanomaly(dataframe, mean):
    lower_bound = mean-(mean*0.5)
    upper_bound = mean+(mean*0.5)
    return_dataframe = dataframe.filter((dataframe['count']>upper_bound) | (dataframe['count']<lower_bound))
    return return_dataframe
      
if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("TestDataset")\
        .getOrCreate()
    
    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    # Do It
    yellow_tripdata_df = spark.read.csv("s3a://"+s3_data_repository_bucket+"/ECS765/other_datasets/nyc_taxi/combined-yellow-data.csv",header=True)
    green_tripdata_df = spark.read.csv("s3a://"+s3_data_repository_bucket+"/ECS765/other_datasets/nyc_taxi/combined-green-data.csv",header=True)
    taxi_zone_lookup = spark.read.csv("s3a://" + s3_data_repository_bucket + "/ECS765/other_datasets/nyc_taxi/taxi_zone_lookup.csv",header=True)

    # checking and removing any null values or wrong format in the dataset and cleaning them for further processing
    yellow_tripdata_df = fieldcleansing(yellow_tripdata_df).drop('_c0')
    green_tripdata_df  =  fieldcleansing (green_tripdata_df).drop('_c0')
    # start working the the task as per instruction
    
    #TASK 1
    yellow_tripdata_df = yellow_tripdata_df.join(taxi_zone_lookup, yellow_tripdata_df['PULocationID'] == taxi_zone_lookup['LocationID']).withColumnRenamed('Borough', 'Pickup_Borough').withColumnRenamed('Zone', 'Pickup_Zone').withColumnRenamed('service_zone', 'Pickup_service_zone').drop('LocationID')
    
    yellow_tripdata_df = yellow_tripdata_df.join(taxi_zone_lookup, yellow_tripdata_df['DOLocationID'] == taxi_zone_lookup['LocationID']).withColumnRenamed('Borough', 'Dropoff_Borough').withColumnRenamed('Zone', 'Dropoff_Zone').withColumnRenamed('service_zone', 'Dropoff_service_zone').drop('LocationID').cache()
    
    green_tripdata_df = green_tripdata_df.join(taxi_zone_lookup, green_tripdata_df['PULocationID']== taxi_zone_lookup['LocationID']).withColumnRenamed('Borough', 'Pickup_Borough').withColumnRenamed('Zone', 'Pickup_Zone').withColumnRenamed('service_zone', 'Pickup_service_zone').drop('LocationID')

    green_tripdata_df = green_tripdata_df.join(taxi_zone_lookup, green_tripdata_df['DOLocationID']== taxi_zone_lookup['LocationID']).withColumnRenamed('Borough', 'Dropoff_Borough').withColumnRenamed('Zone','Dropoff_Zone').withColumnRenamed('service_zone', 'Dropoff_service_zone').drop('LocationID').cache()
    
    yellow_tripdata_df.printSchema()
    green_tripdata_df.printSchema()
    print("")
    print("")
    print(f"The number of yellow dataframe rows is {yellow_tripdata_df.count()}")
    print(f"The number of green dataframe columns is {green_tripdata_df.count()}")
    print(f"The number of yellow dataframe rows is {len(yellow_tripdata_df.columns)}")
    print(f"The number of green dataframe columns is {len(green_tripdata_df.columns)}")
    print("")
    print("")
    
    #TASK 2
    yellow_pickup_borough=yellow_tripdata_df.groupBy('Pickup_Borough').count().cache()
    green_pickup_borough=green_tripdata_df.groupBy('Pickup_Borough').count().cache()
    yellow_dropoff_borough=yellow_tripdata_df.groupBy('Dropoff_Borough').count()
    green_dropoff_borough=green_tripdata_df.groupBy('Dropoff_Borough').count()
    
    yellow_pickup_borough.coalesce(1).write.option("header","true").mode("overwrite").csv("s3a://"+s3_bucket+"/Task2_part1")
    green_pickup_borough.coalesce(1).write.option("header","true").mode("overwrite").csv("s3a://"+s3_bucket+"/Task2_part2")
    yellow_dropoff_borough.coalesce(1).write.option("header","true").mode("overwrite").csv("s3a://"+s3_bucket+"/Task2_part3")
    green_dropoff_borough.coalesce(1).write.option("header","true").mode("overwrite").csv("s3a://"+s3_bucket+"/Task2_part4")
    
    #TASK 3
    plot_filter_yellow_trip = filter(yellow_tripdata_df)
    plot_filter_yellow_trip  = plot_filter_yellow_trip.groupBy('tpep_pickup_datetime').count()
    
    plot_filter_yellow_trip.coalesce(1).write.option("header","true").mode("overwrite").csv("s3a://"+s3_bucket+"/Task3")

    #TASK 4
    top5_yellow_pickup=yellow_pickup_borough.orderBy(col("count").desc()).limit(5)
    top5_green_pickup=green_pickup_borough.orderBy(col("count").desc()).limit(5)

    top5_yellow_pickup.coalesce(1).write.option("header","true").mode("overwrite").csv("s3a://"+s3_bucket+"/Task4_part1")
    top5_green_pickup.coalesce(1).write.option("header","true").mode("overwrite").csv("s3a://"+s3_bucket+"/Task4_part2")
    
    #TASK 5
    yellow_June = spark.read.csv("s3a://" + s3_data_repository_bucket + "/ECS765/nyc_taxi/yellow_tripdata/2023/yellow_tripdata_2023-06.csv", header=True)
    yellow_June = fieldcleansing(yellow_June).drop('_c0')
    yellow_June = yellow_June.withColumn("tpep_pickup_datetime",to_date(col("tpep_pickup_datetime")))
    yellow_June = yellow_June.groupBy("tpep_pickup_datetime").count().orderBy(col("tpep_pickup_datetime")).cache()
    total_trips = yellow_June.select(sum('count')).collect()[0][0]
    mean_count = total_trips/yellow_June.count()
    print("")
    print("")
    print(f"The mean is {mean_count}")
    print("")
    print("")
    anomalies = findanomaly(yellow_June, mean_count).orderBy(col("tpep_pickup_datetime"))
    
    yellow_June.coalesce(1).write.option("header","true").mode("overwrite").csv("s3a://"+s3_bucket+"/Task5/all")
    anomalies.coalesce(1).write.option("header","true").mode("overwrite").csv("s3a://"+s3_bucket+"/Task5/anomalies")
    
    #TASK 6
    yellow_March = spark.read.csv("s3a://" + s3_data_repository_bucket + "/ECS765/nyc_taxi/yellow_tripdata/2023/yellow_tripdata_2023-03.csv", header=True)
    yellow_March = fieldcleansing(yellow_March).drop('_c0')
    yellow_March = yellow_March.select(yellow_March['trip_distance'], yellow_March['fare_amount'])
    average_fare = yellow_March.withColumn("fare_per_mile", col("fare_amount")/col("trip_distance")).drop('trip_distance').drop('fare_amount')
    mean = average_fare.select(avg('fare_per_mile')).collect()[0][0]
    print("")
    print("")
    print(f"The mean is {mean}")
    print("")
    print("")    

    average_fare.coalesce(1).write.option("header","true").mode("overwrite").csv("s3a://"+s3_bucket+"/Task6")
    
    #TASK 7
    solo_yellow = yellow_tripdata_df.filter(yellow_tripdata_df['passenger_count']==1.0).count()
    solo_green = green_tripdata_df.filter(green_tripdata_df['passenger_count']==1.0).count()
    total_trip_count_yellow=yellow_tripdata_df.count()
    total_trip_count_green=green_tripdata_df.count()
    print("")
    print("")
    print(f"Solo passengers for the yellow taxi dataset is {solo_yellow/total_trip_count_yellow*100}%")
    print(f"Solo passengers for the green taxi dataset is {solo_green/total_trip_count_green*100}%")
    print("")
    print("")

    #TASK 8
    yellow_Jan = spark.read.csv("s3a://" + s3_data_repository_bucket + "/ECS765/nyc_taxi/yellow_tripdata/2023/yellow_tripdata_2023-01.csv", header=True)
    yellow_Jan = yellow_Jan.withColumn("tpep_pickup_datetime",(unix_timestamp(yellow_Jan['tpep_pickup_datetime'],'yyyy-MM-dd HH:mm:ss')))
    yellow_Jan = yellow_Jan.withColumn("tpep_dropoff_datetime",(unix_timestamp(yellow_Jan['tpep_dropoff_datetime'],'yyyy-MM-dd HH:mm:ss')))
    yellow_Jan = yellow_Jan.withColumn('trip_duration', round((yellow_Jan["tpep_dropoff_datetime"] - yellow_Jan["tpep_pickup_datetime"])/360))
    plot_task_8 = yellow_Jan.select(yellow_Jan['trip_duration'], yellow_Jan['trip_distance'])
    
    plot_task_8.coalesce(1).write.option("header","true").mode("overwrite").csv("s3a://"+s3_bucket+"/Task8")    

    #TASK 9
    top5_yellow=yellow_tripdata_df.select('Pickup_Borough', 'taxi_type')
    top5_green=green_tripdata_df.select('Pickup_Borough', 'taxi_type')
    
    top5_yellow=top5_yellow.withColumn('Borough', concat(top5_yellow['Pickup_Borough'], lit('_'), top5_yellow['taxi_type'])).groupBy('Borough').count().orderBy(col('count').desc()).limit(5)
    top5_green=top5_green.withColumn('Borough', concat(top5_green['Pickup_Borough'], lit('_'), top5_green['taxi_type'])).groupBy('Borough').count().orderBy(col('count').desc()).limit(5)

    top5_yellow=top5_yellow.withColumn('colour', lit('yellow'))
    top5_green=top5_green.withColumn('colour', lit('green'))
    
    top10all=top5_yellow.union(top5_green).orderBy(col('count').desc())
    
    top10all.coalesce(1).write.option("header","true").mode("overwrite").csv("s3a://"+s3_bucket+"/Task9")
    
    #TASK 10
    task10_yellow = yellow_tripdata_df.select(yellow_tripdata_df["tpep_pickup_datetime"])
    task10_green = green_tripdata_df.select(green_tripdata_df["lpep_pickup_datetime"])
    
    task10_yellow = task10_yellow.withColumn("tpep_pickup_datetime", month(task10_yellow["tpep_pickup_datetime"])).withColumnRenamed("tpep_pickup_datetime", "months")
    task10_green = task10_green.withColumn("lpep_pickup_datetime", month(task10_green["lpep_pickup_datetime"])).withColumnRenamed("lpep_pickup_datetime", "months")
    
    task10_yellow = task10_yellow.groupBy("months").count()
    task10_green = task10_green.groupBy("months").count()
    
    topmonth_yellow = task10_yellow.orderBy(col("count").desc()).limit(1).withColumn('colour', lit('yellow'))
    topmonth_green = task10_green.orderBy(col("count").desc()).limit(1).withColumn('colour', lit('green'))

    topmonth_both=topmonth_green.union(topmonth_yellow)
    
    topmonth_both.coalesce(1).write.option("header","true").mode("overwrite").csv("s3a://"+s3_bucket+"/Task10")
    
    spark.stop()