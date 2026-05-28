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


# Biến lưu trữ giá trị đã làm mượt (đặt ngoài hàm)
smoothed_rain = {f"tuyen{i}": 0 for i in range(1, 4)}

def run_ai_prediction(tuyen_id):
    history = list(data_history[tuyen_id])
    if len(history) < 5: 
        return stations_data[tuyen_id]["val"]

    current_raw_rain = history[-1]["rain_percent"]
    
    # Áp dụng EMA để làm mượt dữ liệu mưa
    # alpha nằm trong khoảng (0, 1). alpha càng nhỏ, dự báo càng "mượt" nhưng chậm phản ứng
    alpha = 0.2 
    smoothed_rain[tuyen_id] = (alpha * current_raw_rain) + ((1 - alpha) * smoothed_rain[tuyen_id])
    
    # Sử dụng smoothed_rain thay vì current_raw_rain
    rain_impact = smoothed_rain[tuyen_id] * 0.05 # Điều chỉnh hệ số 0.05 tùy bạn
    
    # ... (phần code dự báo cũ của bạn)
    prediction = base_prediction + rain_impact
    
    return round(float(max(history[-1]["water"], prediction)), 1)

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
