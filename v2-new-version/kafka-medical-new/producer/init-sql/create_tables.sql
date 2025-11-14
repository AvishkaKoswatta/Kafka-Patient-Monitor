-- CREATE TABLE patient_vitals (
--     patient_id VARCHAR(50),
--     timestamp TIMESTAMP,
--     heart_rate INT,
--     blood_pressure_systolic INT,
--     blood_pressure_diastolic INT,
--     oxygen_saturation INT,
--     PRIMARY KEY (patient_id, timestamp)
-- );

-- INSERT INTO public.patient_vitals 
-- (patient_id, timestamp, heart_rate, blood_pressure_systolic, blood_pressure_diastolic, oxygen_saturation)
-- VALUES (1, NOW(), 80, 120, 80, 98);


-- Create the patients database schema (already created by Docker env var POSTGRES_DB=patients)
-- So we just create the table inside it.

-- Create patient_vitals table
CREATE TABLE patient_vitals (
    patient_id VARCHAR PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    heart_rate INT,
    blood_pressure_systolic INT,
    blood_pressure_diastolic INT,
    oxygen_saturation INT
);


-- Insert some initial dummy data (optional, just for testing)
INSERT INTO public.patient_vitals (patient_id, heart_rate, blood_pressure_systolic, blood_pressure_diastolic, oxygen_saturation)
VALUES
('P001', 85, 120, 80, 96),
('P002', 110, 130, 85, 88),
('P003', 72, 115, 75, 99);
