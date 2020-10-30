from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("mbr_analysis").enableHiveSupport().getOrCreate()

#retrieve data from table.

spark.sql("select mbr.id,mbr.state,mbr.zip, mbr.gender, mbr.birth_date, risk_a.churn_risk,risk_a.sentiment,risk_a.create_date from "
                          "crm_db.crm_mbr mbr inner join crm_db.crm_mbr_risk_analysis risk_a on mbr.id_firstchar = risk_a.id_firstchar "
                          "and mbr.id = risk_a.id").explain()

""" explain plan 
== Physical Plan ==
*(5) Project [id#0, state#11, zip#12, gender#6, birth_date#3, churn_risk#15, sentiment#16, create_date#18]
+- *(5) SortMergeJoin [id_firstchar#13, id#0], [id_firstchar#19, id#14], Inner
   :- *(2) Sort [id_firstchar#13 ASC NULLS FIRST, id#0 ASC NULLS FIRST], false, 0
   :  +- Exchange hashpartitioning(id_firstchar#13, id#0, 200)
   :     +- *(1) Filter isnotnull(id#0)
   :        +- HiveTableScan [id#0, birth_date#3, gender#6, state#11, zip#12, id_firstchar#13], HiveTableRelation `crm_db`.`crm_mbr`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, [id#0, name#1, address#2, birth_date#3, email#4, first_visit#5, gender#6, latitude#7, longitude#8, phone_number#9, ssn#10, state#11, zip#12], [id_firstchar#13], [isnotnull(id_firstchar#13)]
   +- *(4) Sort [id_firstchar#19 ASC NULLS FIRST, id#14 ASC NULLS FIRST], false, 0
      +- Exchange hashpartitioning(id_firstchar#19, id#14, 200)
         +- *(3) Filter isnotnull(id#14)
            +- HiveTableScan [id#14, churn_risk#15, sentiment#16, create_date#18, id_firstchar#19], HiveTableRelation `crm_db`.`crm_mbr_risk_analysis`, org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe, [id#14, churn_risk#15, sentiment#16, persona_cd#17, create_date#18], [id_firstchar#19], [isnotnull(id_firstchar#19)]
+

"""

risk_analysis = spark.sql("select mbr.id,mbr.state,mbr.zip, mbr.gender, mbr.birth_date, risk_a.churn_risk,risk_a.sentiment,risk_a.create_date from "
                          "crm_db.crm_mbr mbr inner join crm_db.crm_mbr_risk_analysis risk_a on mbr.id_firstchar = risk_a.id_firstchar "
                          "and mbr.id = risk_a.id")


#risk_analysis_by_state
risk_analysis.groupBy("state","gender").pivot("sentiment").count().sort("state").show()


""" sample output
+-----+------+--------+-------+--------+
|state|gender|NEGATIVE|NEUTRAL|POSITIVE|
+-----+------+--------+-------+--------+
|   ak|  MALE|      22|     53|      50|
|   ak|FEMALE|      25|     55|      67|
|   ak| OTHER|       1|      2|       1|
|   al|  MALE|     100|    405|     244|
|   al|FEMALE|     101|    347|     234|
|   al| OTHER|       4|     10|      10|
|   ar|FEMALE|      79|    247|     160|
|   ar|  MALE|      81|    245|     150|
|   ar| OTHER|       2|      4|       4|
"""

spark.stop()
