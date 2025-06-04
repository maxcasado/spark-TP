from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

spark = SparkSession.builder \
    .appName("CleanContracts2022") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

df_raw = spark.read.option("multiline", "true") \
    .json("s3a://bronze/sources/aws-marchespublics-annee-2022.json")

df_contracts = df_raw.withColumn("marche", explode(col("marches"))).select("marche.*")

df_contracts_2022 = df_contracts.filter(col("datePublicationDonnees").startswith("2022"))

df_titulaires = df_contracts_2022.withColumn("titulaire", explode(col("titulaires"))) \
    .select(
        col("titulaire.id").alias("siret"),
        col("titulaire.denominationSociale").alias("entreprise"),
        "objet", "montant", "dateNotification", "dureeMois"
    )

df_titulaires.write.mode("overwrite").parquet("s3a://silver/clean_contracts_2022")

spark.stop()
