import os
import sys
import shutil
import datetime

# --- CONFIG WINDOWS ---
HADOOP_BIN_PATH = "C:\\hadoop\\bin"
os.environ['HADOOP_HOME'] = "C:\\hadoop"
if HADOOP_BIN_PATH not in os.environ['PATH']:
    os.environ['PATH'] = HADOOP_BIN_PATH + ";" + os.environ['PATH']

from pyspark.sql import SparkSession
# Ajout des fonctions first, last, max, min pour les chandeliers
from pyspark.sql.functions import from_json, col, window, avg, first, last, max, min, sum
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from influxdb import InfluxDBClient

# Nettoyage checkpoints
checkpoint_dir = "C:\\CryptoProject\\checkpoints"
if os.path.exists(checkpoint_dir):
    try:
        shutil.rmtree(checkpoint_dir)
    except:
        pass

# --- CONFIG SPARK ---
packages = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    "io.delta:delta-spark_2.12:3.0.0",
    "org.apache.hadoop:hadoop-aws:3.3.4"
]

spark = SparkSession.builder \
    .appName("CryptoCandleEngine") \
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

spark.sparkContext.setLogLevel("WARN")

# --- LECTURE KAFKA ---
schema = StructType([
    StructField("timestamp", StringType()),
    StructField("btc_price", FloatType()),
    StructField("news_count", IntegerType()),
    StructField("sentiment_score", FloatType())
])

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crypto-topic") \
    .option("startingOffsets", "latest") \
    .load()

df_clean = df_kafka.select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", col("timestamp").cast("timestamp"))

# --- AGGRÉGATION OHLC (Open, High, Low, Close) ---
# On groupe par fenêtre de 1 minute pour créer les bougies
ohlc_df = df_clean \
    .groupBy(window(col("timestamp"), "1 minute")) \
    .agg(
        first("btc_price").alias("open"),
        max("btc_price").alias("high"),
        min("btc_price").alias("low"),
        last("btc_price").alias("close"),
        avg("sentiment_score").alias("avg_sentiment"),
        sum("news_count").alias("total_news")
    )

# --- ÉCRITURE INFLUXDB ---
def write_ohlc_to_influx(batch_df, batch_id):
    try:
        client = InfluxDBClient(host='localhost', port=8086, database='crypto_project')
        points = []
        # On force l'heure actuelle pour l'affichage temps réel
        now_time = datetime.datetime.utcnow().isoformat()
        
        for row in batch_df.collect():
            points.append({
                "measurement": "market_candles", # Nouvelle table
                "time": now_time,
                "fields": {
                    "open": float(row.open),
                    "high": float(row.high),
                    "low": float(row.low),
                    "close": float(row.close),
                    "sentiment": float(row.avg_sentiment),
                    "news_vol": int(row.total_news)
                }
            })
        if points:
            client.write_points(points)
            print(f"⚡ Batch {batch_id}: OHLC Candle envoyée (O:{row.open} C:{row.close})")
        client.close()
    except Exception as e:
        print(f"⚠️ Erreur Influx: {e}")

query = ohlc_df.writeStream \
    .foreachBatch(write_ohlc_to_influx) \
    .outputMode("update") \
    .start()

spark.streams.awaitAnyTermination()