import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import substring, col, to_date

from utils.MyUtils import myConfigParser

#get config parser
myConfig = myConfigParser()

#read the argument which signifies the execution environment
exeEnv = sys.argv[1]

crmFilePath = myConfig.get(exeEnv,'inputCrmFile')

#creating spark session
spark = SparkSession.builder.appName("load_crm_data").config("spark.hadoop.hive.exec.dynamic.partition", "true") \
    .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
    .enableHiveSupport().getOrCreate()

#reading crm file
crm_data = spark.read.format("json").option("inferSchema","true").load(crmFilePath)

spark.sql("show databases")
spark.sql("use crm_db")
#spark.sql("set hive.exec.dyanmic.partition = true")
#spark.sql("set hive.exec.dynamic.partition.mode = nonstrict")


#changing data for date fields
crm_data = crm_data.withColumn("birth_date",to_date(col('birth_date'),"MM/dd/yyyy")) \
    .withColumn("first_visit",to_date(col('first_visit'),"MM/dd/yyyy")) \
    .withColumn("id_firstchar",substring(col('id'),1,1))

#loading into the table
crm_data.select("id","name","address","birth_date","email","first_visit","gender" ,"latitude","longitude","phone_number","ssn","state","zip","id_firstchar") \
    .write.mode("append").insertInto("crm_mbr")




spark.stop()




