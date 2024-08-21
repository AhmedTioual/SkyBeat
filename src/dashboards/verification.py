from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("VerifyTable").getOrCreate()

print(spark.sql("SHOW DATABASES").show())
