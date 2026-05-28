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
# Sửa dòng này trong app.py
MQTT_TOPIC_SUB = "iuh/phongngap/#"
MQTT_TOPIC_PUB_PREFIX = "iuh/phongngap/control/"

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




def run_ai_prediction_simple(tuyen_id):
    history = list(data_history[tuyen_id])
    if len(history) < 5:
        return history[-1]["water"] if history else 0
    
    # Lấy 5 mẫu gần nhất
    recent = history[-5:]
    
    # Tính gradient (tốc độ thay đổi trung bình)
    water_levels = [data["water"] for data in recent]
    rain_values = [data["rain_percent"] for data in recent]
    
    # Nếu đang có mưa (rain > 30%)
    if rain_values[-1] > 30:
        # Tính tốc độ tăng trung bình
        if water_levels[-1] > water_levels[0]:
            avg_increase = (water_levels[-1] - water_levels[0]) / 4
            # Dự đoán T+5 với tốc độ tăng hiện tại
            prediction = water_levels[-1] + avg_increase * 5
        else:
            # Nước đang giảm nhưng có mưa, giảm chậm hơn
            avg_decrease = (water_levels[0] - water_levels[-1]) / 4
            prediction = max(water_levels[-1], water_levels[-1] - avg_decrease * 2)
    else:
        # Không mưa, nước sẽ rút dần
        if water_levels[-1] > water_levels[0]:
            # Đang xuống dốc
            avg_decrease = (water_levels[-1] - water_levels[0]) / 4
            prediction = water_levels[-1] - avg_decrease * 5
        else:
            prediction = water_levels[-1]
    
    # Nếu có mưa lớn, đảm bảo dự đoán không thấp hơn hiện tại
    if rain_values[-1] > 50:
        prediction = max(prediction, water_levels[-1])
    
    return round(float(max(0, prediction)), 1)

def on_message(client, userdata, msg):
    try:
        topic = msg.topic # Ví dụ: iuh/phongngap/tuyen1/nuoc
        payload_data = json.loads(msg.payload.decode()) # Ví dụ: {"water":0, "rain":100}

        # Trích xuất tên trạm từ topic
        # Giả sử topic luôn có dạng iuh/phongngap/tuyenX/...
        parts = topic.split('/')
        if len(parts) >= 3:
            t_id = parts[2] # Kết quả sẽ là "tuyen1", "tuyen2"...
            
            water_level = float(payload_data.get("water", 0))
            rain_percent = float(payload_data.get("rain", 0))

            if t_id in stations_data:
                data_history[t_id].append({"water": water_level, "rain_percent": rain_percent})
                stations_data[t_id]["val"] = water_level
                stations_data[t_id]["rain_percent"] = rain_percent
                stations_data[t_id]["pred"] = run_ai_prediction(t_id)
                stations_data[t_id]["last_seen"] = time.time()
    except Exception as e:
        print(f"Lỗi xử lý tin nhắn: {e}")
    


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
