from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("create_metadata").config("spark.hadoop.hive.exec.dynamic.partition", "true") \
    .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
    .enableHiveSupport().getOrCreate()

spark.sql("create database if not exists crm_db")

spark.sql("use crm_db")

spark.sql("drop table if exists crm_mbr")
spark.sql("drop table if exists crm_mbr_risk_analysis")

spark.sql("create table if not exists crm_mbr (id String,name string, address String, birth_date Date, email String, first_visit String, gender String,"
          " latitude String, longitude String,phone_number String, ssn String, state String, zip String) partitioned by (id_firstchar String)")

spark.sql("create table if not exists crm_mbr_risk_analysis (id String, churn_risk Int, sentiment String,persona_cd int, create_date Date) partitioned by (id_firstchar String)")

spark.stop()