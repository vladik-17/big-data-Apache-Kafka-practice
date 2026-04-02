"""
kafka/batch_producer.py
Читает dataset.csv (погода в Торжке) и отправляет каждую строку
как JSON-сообщение в топик raw-data.
"""

import csv
import json
import logging
import time
import os
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Настройки ──────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = "127.0.0.1:9092"
TOPIC = "raw-data"

# Путь к датасету — ищем на уровень выше (рядом с папкой kafka/)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATASET_PATH = os.path.join(BASE_DIR, "dataset.csv")

# Задержка между сообщениями (сек). 0 = максимальная скорость.
SEND_DELAY = 0.01

# Сколько строк отправить. None = все строки датасета.
LIMIT = None

# ── Логирование ────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [producer] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Подключение к Kafka ────────────────────────────────────────────────────

def create_producer() -> KafkaProducer:
    """Создать producer с повторными попытками при старте."""
    for attempt in range(1, 6):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                # Сериализуем значение в JSON-байты
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                # Ключ — обычная строка
                key_serializer=lambda k: k.encode("utf-8"),
                # Ждём подтверждения от лидера партиции
                acks="all",
            )
            log.info("Подключились к Kafka: %s", KAFKA_BOOTSTRAP)
            return producer
        except NoBrokersAvailable:
            log.warning("Попытка %d/5: Kafka недоступна, ждём 3 сек...", attempt)
            time.sleep(3)
    raise RuntimeError("Не удалось подключиться к Kafka после 5 попыток.")

# ── Чтение датасета ────────────────────────────────────────────────────────

def read_dataset(path: str):
    """Генератор: читает CSV и отдаёт строки как словари."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Датасет не найден: {path}")

    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield dict(row)

# ── Отправка ───────────────────────────────────────────────────────────────

def send_batch(producer: KafkaProducer, limit=None) -> int:
    """Отправить все (или limit) строк датасета в Kafka."""
    sent = 0
    errors = 0

    for row in read_dataset(DATASET_PATH):
        if limit and sent >= limit:
            break

        # Добавляем метку источника — consumer будет её различать
        message = {**row, "source": "batch"}

        try:
            future = producer.send(
                TOPIC,
                key="source_batch",
                value=message,
            )
            # Ждём подтверждения (блокирующий вызов для надёжности)
            future.get(timeout=10)
            sent += 1

            if sent % 500 == 0:
                log.info("Отправлено: %d строк", sent)

            if SEND_DELAY:
                time.sleep(SEND_DELAY)

        except Exception as e:
            errors += 1
            log.error("Ошибка при отправке строки %d: %s", sent + 1, e)
            # Продолжаем, не останавливаемся на одной ошибке
            continue

    producer.flush()
    return sent, errors

# ── Точка входа ────────────────────────────────────────────────────────────

def main():
    log.info("=== Batch producer запущен ===")
    log.info("Топик: %s | Датасет: %s", TOPIC, DATASET_PATH)

    producer = create_producer()

    try:
        sent, errors = send_batch(producer, limit=LIMIT)
        log.info("=== Готово: отправлено %d, ошибок %d ===", sent, errors)
    finally:
        producer.close()
        log.info("Producer закрыт.")


if __name__ == "__main__":
    main()
