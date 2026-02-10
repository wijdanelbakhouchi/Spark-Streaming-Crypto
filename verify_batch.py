import os
import sys

# --- CONFIGURATION WINDOWS (Même que le processor) ---
HADOOP_BIN_PATH = "C:\\hadoop\\bin"
os.environ['HADOOP_HOME'] = "C:\\hadoop"
if HADOOP_BIN_PATH not in os.environ['PATH']:
    os.environ['PATH'] = HADOOP_BIN_PATH + ";" + os.environ['PATH']

from pyspark.sql import SparkSession

# --- CONFIG SPARK ---
packages = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    "io.delta:delta-spark_2.12:3.0.0",
    "org.apache.hadoop:hadoop-aws:3.3.4"
]

print("⏳ Démarrage de l'analyse historique...")

spark = SparkSession.builder \
    .appName("CryptoBatchVerifier") \
    .config("spark.jars.packages", ",".join(packages)) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true") \
    .config("spark.driver.extraLibraryPath", HADOOP_BIN_PATH) \
    .config("spark.executor.extraLibraryPath", HADOOP_BIN_PATH) \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# --- LECTURE DU DATA LAKE (MinIO) ---
print("📂 Lecture des fichiers Delta depuis MinIO...")

try:
    # On lit le dossier où le streaming écrit
    df_history = spark.read.format("delta").load("s3a://crypto-bucket/data/raw")
    
    # On affiche les statistiques
    count = df_history.count()
    print(f"✅ Succès ! Il y a {count} enregistrements stockés dans le Data Lake.")
    
    print("\n--- 📊 Aperçu des 20 dernières données stockées ---")
    df_history.orderBy(df_history.timestamp.desc()).show(20, truncate=False)
    
    print("\n--- 📈 Analyse Moyenne par Minute (Post-Traitement) ---")
    df_history.createOrReplaceTempView("crypto_data")
    spark.sql("""
        SELECT 
            window(timestamp, '1 minute').start as time_window,
            avg(btc_price) as avg_price,
            avg(sentiment_score) as avg_sentiment,
            sum(news_count) as total_news
        FROM crypto_data 
        GROUP BY window(timestamp, '1 minute')
        ORDER BY time_window DESC
    """).show(5, truncate=False)

except Exception as e:
    print(f"❌ Erreur de lecture : {e}")

print("🏁 Analyse terminée.")