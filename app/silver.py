import pyspark.sql.functions as F

from pyspark.sql.window import Window
from pyspark.sql import SparkSession

def column_convert(df, column, datatype):
        df = df.withColumn(column, F.col(column).cast(datatype))
        return df

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
            .format("delta")
            .load("s3a://live-una-workshop-a3data/bronze/customers/")
    )

    df_vehicle = (
        spark
            .read
            .format("delta")
            .load("s3a://live-una-workshop-a3data/bronze/vehicle/")
    )

    df_credit_score = (
        spark
            .read
            .format("delta")
            .load("s3a://live-una-workshop-a3data/bronze/credit-score/")
    )


    df = (
        df_customers.alias("main")
            .join(df_vehicle.alias("vehicle"), F.col("id") == F.col("vehicle.customer_id"), how="left")
            .join(df_credit_score.alias("credit"), F.col("id") == F.col("credit.customer_id"), how="left")
        )



    df = (
        df.select(
        "id",
        "main.nome",
        "sexo",
        "endereco",
        "email",
        "nascimento",
        "profissao",
        "provedor",
        "credit_score",
        "modelo",
        "fabricante",
        "ano_veiculo",
        "categoria"
        )
    )

    schema = {
        "id": "int",
        "ano_veiculo": "int",
        "credit_score": "int",
        "nascimento": "date"
    }

    for column,datatype in schema.items():
        df = column_convert(df, column, datatype)

    df.coalesce(1).write.mode("overwrite").format("delta").save("s3a://live-una-workshop-a3data/silver/leads")

    spark.stop()