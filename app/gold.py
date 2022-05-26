import pyspark.sql.functions as F

from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

if __name__ == "__main__":

    spark = (
        SparkSession
            .builder
            .appName("live-app")
            .getOrCreate()
    )

    spark.sparkContext.setLogLevel("INFO")

    df = (
        spark
            .read
            .format("delta")
            .load("s3a://live-una-workshop-a3data/silver/leads")
    )

    df.createOrReplaceTempView("vw_leads")

    df = spark.sql("""
    SELECT 
        nome, 
        endereco, 
        email, 
        profissao, 
        FLOOR(DATEDIFF(CURRENT_DATE, nascimento) / 365.25) as idade,
        CASE 
            WHEN FLOOR(DATEDIFF(CURRENT_DATE, nascimento) / 365.25) > 60 THEN 'Elder'
            WHEN FLOOR(DATEDIFF(CURRENT_DATE, nascimento) / 365.25) > 21 THEN 'Adult'
            ELSE 'Young'
        END as faixa_etaria,
        ROUND(credit_score) as media_credito,
        CASE 
            WHEN ROUND(credit_score) > 800 THEN 'Highest'
            WHEN ROUND(credit_score) > 750 THEN 'High'
            WHEN ROUND(credit_score) > 550 THEN 'Medium'
            WHEN ROUND(credit_score) > 400 THEN 'Low'
            ELSE 'Lowest'
        END as categoria
    FROM 
        vw_leads
    WHERE 
        FLOOR(DATEDIFF(CURRENT_DATE, nascimento) / 365.25) >= 18
    AND
        FLOOR(DATEDIFF(CURRENT_DATE, nascimento) / 365.25) <= 80
          """)

    df.coalesce(1).write.mode("overwrite").format("delta").save("s3a://live-una-workshop-a3data/gold/dataset")
    deltaTable = DeltaTable.forPath(spark, "s3a://live-una-workshop-a3data/gold/dataset")
    deltaTable.generate("symlink_format_manifest")
