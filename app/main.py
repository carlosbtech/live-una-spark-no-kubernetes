from pyspark.sql import SparkSession
from delta.tables import DeltaTable

if __name__ == '__main__':

    spark = (
        SparkSession
            .builder
            .appName("demo-spark-bronze")
            .getOrCreate()
    )

    spark.sparkContext.setLogLevel("INFO")
   
    df = spark.range(1,100)

    df.write.mode("overwrite").format("delta").save("s3a://live-una-workshop-a3data/fake_data/")

    delta_table = DeltaTable.forPath(spark, "s3a://live-una-workshop-a3data/fake_data/")
    delta_table.history().show()

    spark.stop()