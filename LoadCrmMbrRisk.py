import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import substring, col, to_date, current_date

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


#adding create date field
crm_data = crm_data.withColumn("id_firstchar",substring(col('id'),1,1)) \
    .withColumn("create_date",current_date())

#loading data to table
crm_data.select("id","churn_risk","sentiment","persona","create_date","id_firstchar") \
    .write.mode("append").insertInto("crm_mbr_risk_analysis")


spark.stop()




