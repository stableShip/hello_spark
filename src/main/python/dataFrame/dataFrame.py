from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession.builder.appName(
        "wordCount").master("local").getOrCreate()
    users = spark.read.json("./user.json")
    users.show()
    users.printSchema()
    users.select("name").show()
    users.select(users["name"], users["age"]+1).show()
    users.filter(users["age"] == 10).show()
    users.filter(users["age"] > 10).show()
    users.groupBy("age").count().show()
