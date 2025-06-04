from pyspark.sql import SparkSession

def main():
    # Démarrage de la session Spark avec config MinIO
    spark = SparkSession.builder \
        .appName("ReadPublicContracts2022") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minio") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    # Lire les données Parquet depuis le bucket gold
    df = spark.read.json("s3a://gold/public_contracts_2022_active")

    # Affichage dans la console (limité à 20 lignes)
    df.show(20)

    # Facultatif : sauvegarde en CSV lisible dans silver
    df.write.mode("overwrite").option("header", "true") \
        .csv("s3a://silver/public_contracts_2022_active_csv")

    spark.stop()

if __name__ == "__main__":
    main()
