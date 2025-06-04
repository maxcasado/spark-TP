from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("CleanEtablissements") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

df_etab = spark.read.option("header", True).csv("s3a://bronze/sources/simulated_etablissements_50000.csv")

df_etab_actifs = df_etab.filter(col("etatAdministratifEtablissement") == "A")

df_etab_actifs.write.mode("overwrite").parquet("s3a://silver/clean_etablissements")

spark.stop()
