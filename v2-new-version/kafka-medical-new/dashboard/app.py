from flask import Flask, render_template, Response
from kafka import KafkaConsumer
import json

app = Flask(__name__)

# Kafka consumer reading Spark output topic OR Debezium topic
consumer = KafkaConsumer(
    "processed_vitals",              # You will create this topic from Spark
    bootstrap_servers="kafka:9092",
    auto_offset_reset="latest",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    consumer_timeout_ms=1000
)

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/stream")
def stream():
    def event_stream():
        consumer = KafkaConsumer(
            "dbserver1.public.patient_vitals",
            bootstrap_servers="kafka:9092",
            auto_offset_reset="earliest",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            group_id=None
        )

        for msg in consumer:
            data = msg.value.get("after")  # only send the actual row
            if data:
                yield f"data: {json.dumps(data)}\n\n"

    return Response(event_stream(), mimetype="text/event-stream")



if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=False)
