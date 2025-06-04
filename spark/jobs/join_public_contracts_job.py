from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("JoinContractsAndEtablissements") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

df_contracts = spark.read.json("s3a://silver/clean_contracts_2022")
df_etabs = spark.read.json("s3a://silver/clean_etablissements")

df_joined = df_contracts.join(df_etabs, on="siret", how="inner") \
    .select(
        "entreprise", "siret", "objet", "montant", "dateNotification",
        "codePostalEtablissement", "libelleCommuneEtablissement"
    )

df_joined.write.mode("overwrite").json("s3a://gold/public_contracts_2022_active_json")

spark.stop()
