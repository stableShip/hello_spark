from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession.builder.appName(
        "wordCount").master("local").getOrCreate()
    users = spark.read.json("./user.json")
    users.createOrReplaceTempView("user")
    sqlDF = spark.sql("select * from user")
    sqlDF.show()
    sqlDF = spark.sql("select * from user where name='jie'")
    sqlDF.show()
