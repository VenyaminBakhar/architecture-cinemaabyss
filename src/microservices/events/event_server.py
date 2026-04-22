import os
import time
import json
import threading
from flask import Flask, request, jsonify
from kafka import KafkaProducer, KafkaConsumer

app = Flask(__name__)

KAFKA_BROKERS = os.environ.get('KAFKA_BROKERS', 'kafka:9092')
TOPICS = ['user-events', 'movie-events', 'payment-events']

def get_kafka_producer():
    print(f"Ожидание готовности Kafka-брокера {KAFKA_BROKERS} (Producer)...")
    max_retries = 5

    for attempt in range(1, max_retries + 1):
        try:
            prod = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print(f"Producer успешно подключен к {KAFKA_BROKERS}")
            return prod
        except Exception as e:
            print(f"Попытка {attempt}/{max_retries}: Producer не смог подключиться ({e})")
            if attempt < max_retries:
                time.sleep(3)

    print("Не удалось подключить Producer после 5 попыток.")
    return None

producer = get_kafka_producer()

def send_to_kafka(topic, data):
    if not producer:
        print("Запрос отклонен, так как Producer не инициализирован")
        return False
    try:
        producer.send(topic, value=data)
        producer.flush()
        return True
    except Exception as e:
        print(f"Ошибка отправки сообщения в Kafka: {e}")
        return False

def run_consumer():
    print(f"Consumer: ожидание готовности Kafka {KAFKA_BROKERS}...")
    max_retries = 5
    consumer = None

    for attempt in range(1, max_retries + 1):
        try:
            consumer = KafkaConsumer(
                *TOPICS,
                bootstrap_servers=KAFKA_BROKERS,
                group_id='events-logger-group',
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            print(f"Consumer успешно подключен и слушает: {', '.join(TOPICS)}")
            break
        except Exception as e:
            print(f"Попытка {attempt}/{max_retries}: Consumer не смог подключиться ({e})")
            if attempt < max_retries:
                time.sleep(3)

    if not consumer:
        print("Консьюмер отключен из-за отсутствия связи с Kafka.")
        return

    try:
        for message in consumer:
            print(f"[CONSUMER] Topic: {message.topic} | Value: {message.value}")
    except Exception as e:
        print(f"Ошибка чтения сообщений в Consumer: {e}")

consumer_thread = threading.Thread(target=run_consumer, daemon=True)
consumer_thread.start()

@app.route('/api/events/health', methods=['GET'])
def health_check():
    return jsonify({"status": True}), 200

@app.route('/api/events/<event_type>', methods=['POST'])
def handle_events(event_type):
    if event_type not in ['user', 'movie', 'payment']:
        return jsonify({"error": f"Endpoint /api/events/{event_type} not found"}), 404

    data = request.get_json(silent=True)
    if data is None:
        return jsonify({"error": "Invalid JSON format or empty body"}), 400

    topic_name = f"{event_type}-events"
    success = send_to_kafka(topic_name, data)

    if success:
        return jsonify({"status": "success"}), 201
    else:
        return jsonify({"status": "error"}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8082))
    app.run(host='0.0.0.0', port=port, use_reloader=False)