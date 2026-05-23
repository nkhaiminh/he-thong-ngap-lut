import paho.mqtt.client as mqtt
from flask import Flask, render_template, jsonify, request
import threading
import time
import random
import json
import collections
import numpy as np
from sklearn.linear_model import LinearRegression
from flask_cors import CORS # 1. Import thư viện
app = Flask(__name__)
CORS(app) # 2. Thêm dòng này sau khi tạo app Flask
MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 1883
MQTT_TOPIC_SUB = "hcmut/phongngap/#"
MQTT_TOPIC_PUB_PREFIX = "hcmut/phongngap/control/"

app = Flask(__name__)

# Lưu lịch sử 30 mẫu gần nhất
data_history = {f"tuyen{i}": collections.deque(maxlen=30) for i in range(1, 4)}

stations_data = {
    f"tuyen{i}": {
        "val": 0,
        "rain_percent": 0,
        "pred": 0,
        "last_seen": 0,
        "status_conn": "Mất kết nối",
        "barrier": "Tự động"
    } for i in range(1, 4)
}


def run_ai_prediction(tuyen_id):
    history = list(data_history[tuyen_id])
    if len(history) < 5: return 0

    #Dự báo mức nước dựa trên (Thời gian, Cường độ mưa)
    X = np.array([[i, data["rain_percent"]] for i, data in enumerate(history)])
    y = np.array([data["water"] for data in history]).reshape(-1, 1)

    model = LinearRegression().fit(X, y)

    # Dự báo T+5, giả định cường độ mưa giữ nguyên như hiện tại
    current_rain = history[-1]["rain_percent"]
    future_X = np.array([[len(history) + 5, current_rain]])
    prediction = model.predict(future_X)[0][0]

    return round(float(max(0, prediction)), 1)


def on_message(client, userdata, msg):
    try:
        topic = msg.topic
        payload_data = json.loads(msg.payload.decode())

        water_level = float(payload_data.get("water", 0))
        rain_percent = float(payload_data.get("rain", 0))

        for i in range(1, 4):
            t_id = f"tuyen{i}"
            if t_id in topic:
                # Lưu vào lịch sử cho AI
                data_history[t_id].append({"water": water_level, "rain_percent": rain_percent})

                # Cập nhật dữ liệu hiển thị
                stations_data[t_id]["val"] = water_level
                stations_data[t_id]["rain_percent"] = rain_percent
                stations_data[t_id]["pred"] = run_ai_prediction(t_id)
                stations_data[t_id]["last_seen"] = time.time()
    except Exception as e:
        pass


pi_client_id = f"Pi_Master_{random.randint(1000, 9999)}"
mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, pi_client_id)


def mqtt_thread():
    mqtt_client.on_message = on_message
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
    mqtt_client.subscribe(MQTT_TOPIC_SUB)

    while True:
        try:
            mqtt_client.loop_forever()
        except:
            print("Mat ket noi MQTT. Dang thu lai...")
            time.sleep(5)
            mqtt_client.reconnect()


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/update')
def update_data():
    current_time = time.time()
    for t_id in stations_data:
        if stations_data[t_id]["last_seen"] == 0 or (current_time - stations_data[t_id]["last_seen"] > 10):
            stations_data[t_id]["status_conn"] = "Mất kết nối"
        else:
            stations_data[t_id]["status_conn"] = "Trực tuyến"

    return jsonify(stations_data)

@app.route('/api/control', methods=['POST'])
def control():
    data = request.json
    tuyen = data.get('tuyen')
    action = data.get('action')

    mqtt_client.publish(f"{MQTT_TOPIC_PUB_PREFIX}{tuyen}", action)

    if action == "ON":
        stations_data[tuyen]["barrier"] = "ĐÓNG (Thủ công)"
    elif action == "OFF":
        stations_data[tuyen]["barrier"] = "MỞ (Thủ công)"
    else:
        stations_data[tuyen]["barrier"] = "Tự động"

    return jsonify({"status": "Success"})


if __name__ == '__main__':
    threading.Thread(target=mqtt_thread, daemon=True).start()
    app.run(host='0.0.0.0', port=5000, debug=False)
