from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws

spark = SparkSession.builder \
    .appName("JoinContractsAndEtablissements") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

df_contracts = spark.read.parquet("s3a://silver/clean_contracts_2022")
df_etabs = spark.read.parquet("s3a://silver/clean_etablissements")

df_joined = df_contracts.join(df_etabs, on="siret", how="inner")
#on etabs add complementAdresseEtablissement, numeroVoieEtablissement, indiceRepetitionEtablissement, typeVoieEtablissement, libelleVoieEtablissement, codePostalEtablissement, libelleCommuneEtablissement
df_result = df_joined.withColumn(
    "adresse",
    concat_ws(" ", col("complementAdresseEtablissement"),col("numeroVoieEtablissement"),col("indiceRepetitionEtablissement"),col("typeVoieEtablissement"),col("libelleVoieEtablissement"),col("codePostalEtablissement"), col("libelleCommuneEtablissement"))
)

df_result = df_result.select(
    "entreprise", "siret","dateNotification", "adresse" # "objet", "montant", 
).dropDuplicates(["siret"])

df_result.write.mode("overwrite").json("s3a://gold/public_contracts_2022_active_json")

spark.stop()