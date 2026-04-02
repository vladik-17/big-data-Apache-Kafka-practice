# Kafka → HDFS Pipeline

Погодные данные Торжка → Batch Producer → Kafka → Consumer → HDFS

## Стек
- Python 3 + kafka-python
- Kafka + Zookeeper (Docker)
- Cloudera VM (HDFS)

## Запуск

### 1. Установка
```bash
pip install kafka-python
```

### 2. Kafka
```bash
docker-compose up -d
docker exec kafka-pract kafka-topics --create --bootstrap-server 127.0.0.1:9092 --topic raw-data --partitions 1 --replication-factor 1
```

### 3. Consumer (терминал 1)
```bash
python consumer_hdfs.py
```

### 4. Producer (терминал 2)
```bash
python batch_producer.py
```

### 5. Загрузка в HDFS (внутри Cloudera VM)
```bash
~/hdfs_upload.sh
```

### 6. Проверка
```bash
hdfs dfs -ls /user/cloudera/raw_data/source=batch
```
