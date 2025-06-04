from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MedaillonTransform").getOrCreate()

# Lecture fichier CSV depuis MinIO (bronze)
df = spark.read.csv("s3a://bronze/nom_du_fichier.csv", header=True)
# Exemple simple: filtre les lignes où une colonne 'value' > 10
df_filtered = df.filter(df["value"] > 10)

# Écrit le résultat dans silver
df_filtered.write.mode("overwrite")\
    .csv("s3a://silver/output/", header=True)

spark.stop()
