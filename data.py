import psycopg2
import random
import time
from datetime import datetime

# PostgreSQL connection config
DB_CONFIG = {
    "dbname": "medical_db",
    "user": "postgres",
    "password": "abc",
    "host": "localhost",
    "port": 5432,
}

# Predefined targets
TARGET_BP = "120/80"
TARGET_HR = 75
HEALTH_STATUSES = ["Healthy", "Observation", "Unhealthy"]

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def insert_data(records):
    conn = get_connection()
    cur = conn.cursor()

    query = """
        INSERT INTO patient_sensor_data (
            patient_id, timestamp, sensor_id, sensor_type,
            temperature, systolic_bp, diastolic_bp, heart_rate,
            device_battery_level, target_blood_pressure, target_heart_rate,
            target_health_status, battery_level
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """

    cur.executemany(query, records)
    conn.commit()
    cur.close()
    conn.close()

def generate_record():
    patient_id = random.randint(1000, 2000)
    sensor_id = random.randint(1, 10)
    timestamp = datetime.now()

    # Sensor type selection
    sensor_types = ["Temperature", "Blood Pressure", "Heart Rate"]
    sensor_type = random.choice(sensor_types)

    # Generate values
    temperature = round(random.uniform(36.0, 39.0), 1)
    systolic_bp = random.uniform(100, 160)
    diastolic_bp = random.uniform(60, 100)
    heart_rate = random.randint(60, 120)

    device_battery_level = random.randint(50, 100)   
    battery_level = device_battery_level - random.randint(0, 5)  

    target_bp = TARGET_BP
    target_hr = TARGET_HR
    target_health_status = random.choice(HEALTH_STATUSES)

    return (
        patient_id, timestamp, sensor_id, sensor_type,
        temperature, systolic_bp, diastolic_bp, heart_rate,
        device_battery_level, target_bp, target_hr,
        target_health_status, battery_level
    )

def main():
    while True:
        # Random number of records (1–5 patients at once)
        num_records = random.randint(1, 5)
        records = [generate_record() for _ in range(num_records)]

        insert_data(records)
        print(f"Inserted {num_records} record(s) at {datetime.now()}")

        # Irregular delay (1–8 sec)
        time.sleep(random.uniform(1, 8))

if __name__ == "__main__":
    main()
