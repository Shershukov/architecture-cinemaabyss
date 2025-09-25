from http.server import HTTPServer, BaseHTTPRequestHandler
import os
import sys
import json
import threading
import time
from collections import deque
from datetime import datetime, timezone
from kafka import KafkaProducer, KafkaConsumer

PORT = int(os.getenv("PORT", "8082"))
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BROKERS", "kafka:9092")

TOPICS = {
    "user": "user-events",
    "payment": "payment-events",
    "movie": "movie-events"
}

event_buffers = {
    "user": deque(maxlen=10),
    "payment": deque(maxlen=10),
    "movie": deque(maxlen=10)
}

shutdown_event = threading.Event()

class EventHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path == "/api/events/user":
            event_type = "user"
        elif self.path == "/api/events/payment":
            event_type = "payment"
        elif self.path == "/api/events/movie":
            event_type = "movie"
        else:
            self.send_error(404)
            return

        content_length = int(self.headers.get("Content-Length", 0))
        if content_length == 0:
            self.send_error(400, "Empty request body")
            return

        body = self.rfile.read(content_length)
        if not body.strip():
            self.send_error(400, "Empty request body")
            return

        try:
            event = json.loads(body.decode("utf-8"))
        except json.JSONDecodeError:
            self.send_error(400, "Invalid JSON")
            return

        if not isinstance(event, dict):
            self.send_error(400, "Event must be a JSON object")
            return

        event["_timestamp"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        event["_type"] = event_type

        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
            )
            producer.send(TOPICS[event_type], event)
            producer.flush()
            producer.close()

            self.send_response(201)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "success"}).encode("utf-8"))

        except Exception as e:
            print(f"Kafka error: {e}", file=sys.stderr)
            self.send_error(500, "Failed to send event to Kafka")

    def do_GET(self):
        if self.path == "/api/events/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.end_headers()
            self.wfile.write(json.dumps({"status": True}, ensure_ascii=False).encode("utf-8"))
            return
        if self.path == "/api/events/user":
            events = list(event_buffers["user"])
        elif self.path == "/api/events/payment":
            events = list(event_buffers["payment"])
        elif self.path == "/api/events/movie":
            events = list(event_buffers["movie"])
        else:
            self.send_error(404)
            return

        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.end_headers()
        self.wfile.write(json.dumps(events, ensure_ascii=False, indent=2).encode("utf-8"))

    def log_message(self, format, *args):
        return


def kafka_consumer_worker():
    topics = list(TOPICS.values())
    max_retries = 10
    retry_delay = 5

    for attempt in range(1, max_retries + 1):
        try:
            print(f"Попытка подключения к Kafka ({attempt}/{max_retries})...", flush=True)
            consumer = KafkaConsumer(
                *topics,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                group_id="events-service",
                # Добавим таймауты для быстрого фейла
                request_timeout_ms=20000,
                session_timeout_ms=10000,
                heartbeat_interval_ms=3000
            )
            print(f"Успешно подключено к Kafka: {KAFKA_BOOTSTRAP_SERVERS}", flush=True)
            break  # Успех — выходим из цикла
        except Exception as e:
            print(f"Ошибка подключения к Kafka (попытка {attempt}): {e}", file=sys.stderr, flush=True)
            if attempt == max_retries:
                print("Превышено количество попыток. Консьюмер остановлен.", file=sys.stderr, flush=True)
                return
            time.sleep(retry_delay)
    else:
        return

    try:
        for message in consumer:
            if shutdown_event.is_set():
                break
            event = message.value
            topic = message.topic

            event_type = None
            for et, t in TOPICS.items():
                if t == topic:
                    event_type = et
                    break

            if event_type and event_type in event_buffers:
                event_buffers[event_type].append(event)
                ts = datetime.now().isoformat()
                print(f"[{ts}] {event_type.upper()}: {event}", flush=True)
    except Exception as e:
        print(f"Ошибка при чтении сообщений: {e}", file=sys.stderr, flush=True)
    finally:
        consumer.close()


def main():
    consumer_thread = threading.Thread(target=kafka_consumer_worker, daemon=True)
    consumer_thread.start()

    server = HTTPServer(("0.0.0.0", PORT), EventHandler)
    print(f"Events service запущен на порту {PORT}", flush=True)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nЗавершение...", flush=True)
    finally:
        shutdown_event.set()
        server.server_close()
        print("Завершено.", flush=True)


if __name__ == "__main__":
    main()