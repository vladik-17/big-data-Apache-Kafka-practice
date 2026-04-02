"""
consumer_hdfs.py
Читает сообщения из топика raw-data и сохраняет их в папку,
которая является общей с Cloudera VM (/media/sf_Downloads/hdfs_upload/).
Скрипт hdfs_upload.sh внутри VM затем кладёт файлы в HDFS.
 
Запуск:  python consumer_hdfs.py
"""
 
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime
 
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
 
# ── Настройки ──────────────────────────────────────────────────────────────
 
KAFKA_BOOTSTRAP = "127.0.0.1:9092"
TOPIC = "raw-data"
GROUP_ID = "hdfs-consumer-group"
 
# Папка сохранения — общая с VM через VirtualBox Shared Folders
# Windows: C:\Users\user\Downloads\hdfs_upload\
# VM:      /media/sf_Downloads/hdfs_upload/
SAVE_DIR = os.path.join(os.path.expanduser("~"), "Downloads", "hdfs_upload")
 
# Сколько сообщений копим перед записью одного файла
BATCH_SIZE = 100
 
# Таймаут ожидания новых сообщений (мс)
POLL_TIMEOUT_MS = 5000
 
# ── Логирование ────────────────────────────────────────────────────────────
 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [consumer] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)
 
# ── Флаг остановки ────────────────────────────────────────────────────────
 
_running = True
 
def _handle_stop(sig, frame):
    global _running
    if _running:
        log.info("Получен сигнал остановки, завершаем работу...")
        _running = False
 
signal.signal(signal.SIGINT, _handle_stop)
 
# ── Сохранение файлов ──────────────────────────────────────────────────────
 
def save_messages(messages: list) -> bool:
    """
    Сохранить пачку сообщений в файл.
    Структура папок имитирует HDFS:
    hdfs_upload/source=batch/date=YYYY-MM-DD/file_<timestamp>.json
    """
    if not messages:
        return True
 
    first = messages[0]
    source = first.get("source", "batch")
    date_str = first.get("date", datetime.now().strftime("%Y-%m-%d"))
 
    # Путь: hdfs_upload/source=batch/date=2000-01-01/
    local_dir = os.path.join(SAVE_DIR, f"source={source}", f"date={date_str}")
    os.makedirs(local_dir, exist_ok=True)
 
    timestamp = int(time.time() * 1000)
    file_path = os.path.join(local_dir, f"file_{timestamp}.json")
 
    with open(file_path, "w", encoding="utf-8") as f:
        for msg in messages:
            f.write(json.dumps(msg, ensure_ascii=False) + "\n")
 
    log.info("Сохранено %d записей → %s", len(messages), file_path)
    return True
 
# ── Подключение к Kafka ────────────────────────────────────────────────────
 
def create_consumer() -> KafkaConsumer:
    """Создать consumer с повторными попытками."""
    for attempt in range(1, 6):
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id=GROUP_ID,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                consumer_timeout_ms=POLL_TIMEOUT_MS,
            )
            log.info("Consumer подключён к Kafka: %s, топик: %s", KAFKA_BOOTSTRAP, TOPIC)
            return consumer
        except NoBrokersAvailable:
            log.warning("Попытка %d/5: Kafka недоступна, ждём 3 сек...", attempt)
            time.sleep(3)
    raise RuntimeError("Не удалось подключиться к Kafka после 5 попыток.")
 
# ── Основной цикл ──────────────────────────────────────────────────────────
 
def run():
    log.info("=== Consumer запущен ===")
    log.info("Сохранение в: %s", SAVE_DIR)
 
    consumer = create_consumer()
    batch = []
    total = 0
 
    try:
        while _running:
            try:
                for message in consumer:
                    if not _running:
                        break
                    batch.append(message.value)
 
                    if len(batch) >= BATCH_SIZE:
                        save_messages(batch)
                        total += len(batch)
                        batch.clear()
 
            except Exception as e:
                log.error("Ошибка при чтении: %s", e)
 
            # Таймаут — сохраняем остаток и выходим
            if batch:
                save_messages(batch)
                total += len(batch)
                batch.clear()
                log.info("Flush по таймауту. Всего: %d", total)
 
            break
 
    finally:
        if batch:
            save_messages(batch)
            total += len(batch)
 
        consumer.close()
        log.info("=== Consumer остановлен. Всего обработано: %d сообщений ===", total)
 
 
if __name__ == "__main__":
    run()