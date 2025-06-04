from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

def main():
    spark = SparkSession.builder \
        .appName("PublicContracts2022") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minio") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    df_marches_raw = spark.read.option("multiline", "true") \
        .json("s3a://bronze/sources/aws-marchespublics-annee-2022.json")

    df_marches = df_marches_raw.withColumn("marche", explode(col("marches"))).select("marche.*")
    df_marches_2022 = df_marches.filter(col("datePublicationDonnees").startswith("2022"))

    df_titulaires = df_marches_2022.withColumn("titulaire", explode(col("titulaires"))) \
        .select(
            col("titulaire.id").alias("siret"),
            col("titulaire.denominationSociale").alias("entreprise"),
            "objet", "montant", "dateNotification", "dureeMois"
        )

    df_etabs = spark.read.option("header", True).option("delimiter", ",") \
        .csv("s3a://bronze/sources/simulated_etablissements_50000.csv")

    df_etabs_actifs = df_etabs.filter(col("etatAdministratifEtablissement") == "A")

    df_joined = df_titulaires.join(df_etabs_actifs, on="siret", how="inner") \
        .select(
            "entreprise", "siret", "objet", "montant", "dateNotification",
            "codePostalEtablissement", "libelleCommuneEtablissement"
        )

    df_joined.write.mode("overwrite").json("s3a://gold/public_contracts_2022_active")

    spark.stop()

if __name__ == "__main__":
    main()
