# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, from_json, when
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, LongType
# import requests
# import time

# # ------------------------------
# # 1. Create Spark session
# # ------------------------------
# spark = SparkSession.builder \
#     .appName("ICU_Patient_Alerts_Stream") \
#     .getOrCreate()
# spark.sparkContext.setLogLevel("WARN")

# # ------------------------------
# # 2. Kafka source
# # ------------------------------
# kafka_bootstrap = "localhost:9092"
# kafka_topic = "medical_server.public.patient_alerts"

# df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", kafka_bootstrap) \
#     .option("subscribe", kafka_topic) \
#     .option("startingOffsets", "earliest") \
#     .load()

# # ------------------------------
# # 3. Define schemas
# # ------------------------------
# record_schema = StructType([
#     StructField("patient_id", IntegerType()),
#     StructField("last_updated", LongType()),  # microseconds
#     StructField("temperature", FloatType()),
#     StructField("systolic_bp", IntegerType()),
#     StructField("diastolic_bp", IntegerType()),
#     StructField("heart_rate", IntegerType()),
#     StructField("oxygen_saturation", FloatType()),
#     StructField("device_battery_level", IntegerType()),
# ])

# debezium_schema = StructType([
#     StructField("before", record_schema),
#     StructField("after", record_schema),
#     StructField("op", StringType()),
#     StructField("ts_ms", LongType()),
#     StructField("ts_us", LongType()),
#     StructField("ts_ns", LongType())
# ])

# # ------------------------------
# # 4. Parse Kafka JSON
# # ------------------------------
# df_parsed = df.selectExpr("CAST(value AS STRING) as json_str") \
#     .select(from_json(col("json_str"), debezium_schema).alias("data")) \
#     .select("data.after.*")

# # ------------------------------
# # 5. Convert last_updated → timestamp
# # ------------------------------
# df_final = df_parsed.withColumn(
#     "last_updated_ts",
#     (col("last_updated") / 1_000_000).cast("timestamp")  # microseconds → seconds
# )

# df_alerts = df_final.withColumn(
#     "alert",
#     when((col("systolic_bp") > 140) | (col("diastolic_bp") > 90), "High BP")
#     .otherwise(
#         when(col("heart_rate") > 120, "Tachycardia")
#         .otherwise(
#             when(col("temperature") > 38, "Fever")
#             .otherwise("Normal")
#         )
#     )
# ).withColumn(
#     "battery_status",
#     when(col("device_battery_level") < 20, "Low Battery").otherwise("OK")
# )

# # ------------------------------
# # 6. Function to send to API
# # ------------------------------
# API_URL = "http://localhost:5000/update"
# MAX_RETRIES = 3

# def send_to_api(batch_df, batch_id):
#     def send_partition(iterator):
#         for row in iterator:
#             row_dict = row.asDict()
#             if row_dict.get("last_updated_ts"):
#                 row_dict["last_updated_ts"] = str(row_dict["last_updated_ts"])

#             for attempt in range(MAX_RETRIES):
#                 try:
#                     response = requests.post(API_URL, json=row_dict, timeout=5)
#                     print(f"[API] Patient {row_dict.get('patient_id')} -> Status {response.status_code}")
#                     break
#                 except Exception as e:
#                     print(f"[API ERROR] Attempt {attempt+1} Patient {row_dict.get('patient_id')} -> {e}")
#                     time.sleep(1)

#     batch_df.foreachPartition(send_partition)

# # ------------------------------
# # 7. Write stream to API
# # ------------------------------
# query_api = df_alerts.writeStream \
#     .foreachBatch(send_to_api) \
#     .outputMode("append") \
#     .option("checkpointLocation", "/home/avishka/data/projects/kafka-medical/spark_checkpoints/api") \
#     .start()

# # ------------------------------
# # 8. Optional: Write to console for debugging
# # ------------------------------
# query_console = df_alerts.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", False) \
#     .start()

# # ------------------------------
# # 9. Wait for termination
# # ------------------------------
# query_api.awaitTermination()
# query_console.awaitTermination()


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, max as spark_max, expr
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, LongType, StringType
import requests
import time

 
spark = SparkSession.builder \
    .appName("ICU_Patient_Alerts_Stream") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

 
kafka_bootstrap = "localhost:9092"
kafka_topic = "medical_server.public.patient_alerts"

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

 
record_schema = StructType([
    StructField("patient_id", IntegerType()),
    StructField("last_updated", LongType()),  # microseconds
    StructField("temperature", FloatType()),
    StructField("systolic_bp", IntegerType()),
    StructField("diastolic_bp", IntegerType()),
    StructField("heart_rate", IntegerType()),
    StructField("oxygen_saturation", FloatType()),
    StructField("device_battery_level", IntegerType()),
])

debezium_schema = StructType([
    StructField("before", record_schema),
    StructField("after", record_schema),
    StructField("op", StringType()),
    StructField("ts_ms", LongType()),
    StructField("ts_us", LongType()),
    StructField("ts_ns", LongType())
])

 
df_parsed = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), debezium_schema).alias("data")) \
    .select("data.after.*")
 
df_final = df_parsed.withColumn(
    "last_updated_ts",
    (col("last_updated") / 1_000_000).cast("timestamp")
)

 
df_alerts = df_final.withColumn(
    "alert",
    when((col("systolic_bp") > 140) | (col("diastolic_bp") > 90), "High BP")
    .otherwise(
        when(col("heart_rate") > 120, "Tachycardia")
        .otherwise(
            when(col("temperature") > 38, "Fever")
            .otherwise("Normal")
        )
    )
).withColumn(
    "battery_status",
    when(col("device_battery_level") < 20, "Low Battery").otherwise("OK")
)

 

API_URL = "http://127.0.0.1:5000/update"   

df_latest_safe = df_alerts \
    .withWatermark("last_updated_ts", "1 hour") \
    .groupBy("patient_id") \
    .agg(
        spark_max("last_updated_ts").alias("last_updated_ts"),
        spark_max("temperature").alias("temperature"),
        spark_max("systolic_bp").alias("systolic_bp"),
        spark_max("diastolic_bp").alias("diastolic_bp"),
        spark_max("heart_rate").alias("heart_rate"),
        spark_max("oxygen_saturation").alias("oxygen_saturation"),
        spark_max("device_battery_level").alias("device_battery_level"),
        spark_max("alert").alias("alert"),
        spark_max("battery_status").alias("battery_status")
    )
df_latest_safe = df_latest_safe.withColumn(
    "alert",
    when((col("systolic_bp") > 140) | (col("diastolic_bp") > 90), "High BP")
    .otherwise(
        when(col("heart_rate") > 120, "Tachycardia")
        .otherwise(
            when(col("temperature") > 38, "Fever")
            .otherwise("Normal")
        )
    )
).withColumn(
    "battery_status",
    when(col("device_battery_level") < 20, "Low Battery").otherwise("OK")
) 
def send_to_api(batch_df, batch_id):
    def send_partition(iterator):
        for row in iterator:
            row_dict = row.asDict()
            
            for key, value in row_dict.items():
                if hasattr(value, "isoformat"):
                    row_dict[key] = value.isoformat()
            try:
                response = requests.post(API_URL, json=row_dict, timeout=5)
                print(f"[API] Patient {row_dict['patient_id']} -> Status {response.status_code}")
            except Exception as e:
                print(f"[API ERROR] Patient {row_dict.get('patient_id', 'Unknown')} -> {e}")

    batch_df.foreachPartition(send_partition)

 
query = df_latest_safe.writeStream \
    .foreachBatch(send_to_api) \
    .outputMode("complete") \
    .option("checkpointLocation", "/home/avishka/spark_checkpoints/api") \
    .start()

spark.streams.awaitAnyTermination()
