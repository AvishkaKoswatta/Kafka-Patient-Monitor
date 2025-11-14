from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder \
    .appName("PatientVitalsStreaming") \
    .getOrCreate()

schema = StructType([
    StructField("patient_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("heart_rate", IntegerType()),
    StructField("blood_pressure_systolic", IntegerType()),
    StructField("blood_pressure_diastolic", IntegerType()),
    StructField("oxygen_saturation", IntegerType())
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "dbserver1.public.patient_vitals") \
    .load()

df_parsed = df.select(from_json(col("value").cast("string"), schema).alias("data"))

df_parsed.writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination()
