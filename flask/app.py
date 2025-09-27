# from flask import Flask, request, jsonify, render_template

# app = Flask(__name__)

# # Store latest data per patient_id
# latest_data = {}

# @app.route('/update', methods=['POST'])
# def update():
#     data = request.get_json()
#     patient_id = data.get('patient_id')
#     if patient_id:
#         latest_data[patient_id] = data
#     print("✅ Received record:", data)
#     return jsonify({"status": "success"}), 200

# @app.route('/')
# def dashboard():
#     # Sort patients by ID and limit 10
#     patients = sorted(latest_data.values(), key=lambda x: x['patient_id'])[:10]
#     return render_template('dashboard.html', patients=patients)

# if __name__ == "__main__":
#     app.run(debug=True)





from flask import Flask, request, render_template
app = Flask(__name__)

 
patients_data = {}   

@app.route("/update", methods=["POST"])
def update_patient():
    data = request.get_json()
    patient_id = data["patient_id"]
    patients_data[patient_id] = data   
    return {"status": "ok"}

@app.route("/")
def dashboard():
     
    return render_template("dashboard.html", patients=list(patients_data.values()))

if __name__ == "__main__":
    app.run(debug=True)
