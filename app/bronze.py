import pyspark.sql.functions as F

from pyspark.sql.window import Window
from pyspark.sql import SparkSession

if __name__ == "__main__":

    spark = (
        SparkSession
            .builder
            .appName("live-app")
            .getOrCreate()
    )

    spark.sparkContext.setLogLevel("INFO")
   
    df_customers = (
        spark
            .read
            .format("csv")
            .option("header", True)
            .option("multiline", True)
            .option("sep", ";")
            .load("s3a://live-una-workshop-a3data/landing-zone/ftp/customers/")
    )

    df_customers.withColumn("id", F.monotonically_increasing_id())

    w = Window.partitionBy("customer_id").orderBy(F.col("customer_id"))

    df_credit_score = (
        spark
            .read
            .format("csv")
            .option("header", True)
            .load("s3a://live-una-workshop-a3data/landing-zone/api/credit-score/")
    )

    df_credit_score = (
        df_credit_score.withColumn("row",
                               F.row_number().over(w)) 
                              .filter(F.col("row") == 1).drop("row")
                            )

    df_vehicle = (
        spark
            .read
            .format("csv")
            .option("header", True)
            .option("sep", "\t")
            .load("s3a://live-una-workshop-a3data/landing-zone/ftp/vehicle/")
    )
    
    df_vehicle = (
    df_vehicle.withColumn("row",
                               F.row_number().over(w)) 
                              .filter(F.col("row") == 1).drop("row"))

    # write delta table
    df_vehicle.coalesce(1).write.mode("overwrite").format("delta").save("s3a://live-una-workshop-a3data/bronze/vehicle")
    df_credit_score.coalesce(1).write.mode("overwrite").format("delta").save("s3a://live-una-workshop-a3data/bronze/credit-score")
    df_customers.coalesce(1).write.mode("overwrite").format("delta").save("s3a://live-una-workshop-a3data/bronze/customers")


    spark.stop()