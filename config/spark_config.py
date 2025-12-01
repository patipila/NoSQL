"""
Konfiguracja Apache Spark dla projektu NYC Taxi Analysis
Zoptymalizowana pod kątem pamięci dla modeli ML
"""
import os

# Konfiguracja Spark Session
SPARK_CONFIG = {
    "app_name": "NYC_Taxi_Analysis_2016",
    "master": "spark://spark-master:7077",
    
    # Driver networking (CRITICAL for Docker)
    "spark.driver.host": "jupyter",
    "spark.driver.bindAddress": "0.0.0.0",
    "spark.driver.port": "7001",
    "spark.blockManager.port": "7002",
    
    # ========== ZWIĘKSZONA PAMIĘĆ (KLUCZOWE) ==========
    "spark.executor.memory": "2g",           # Zwiększone z 1g
    "spark.driver.memory": "3g",             # Zwiększone z 1g (driver potrzebuje więcej dla ML)
    "spark.executor.cores": "1",
    "spark.driver.maxResultSize": "2g",      # Maksymalny rozmiar wyników
    
    # ========== MEMORY MANAGEMENT ==========
    "spark.memory.fraction": "0.8",          # 80% pamięci dla execution i storage
    "spark.memory.storageFraction": "0.3",   # 30% z tego na storage
    "spark.executor.memoryOverhead": "512m", # Dodatkowa pamięć dla JVM
    "spark.driver.memoryOverhead": "512m",
    
    # ========== GARBAGE COLLECTION ==========
    "spark.executor.extraJavaOptions": "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThreads=2",
    "spark.driver.extraJavaOptions": "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThreads=2",
    
    # Partitions and shuffle
    "spark.sql.shuffle.partitions": "100",   # Zmniejszone z 200 (mniej partycji = mniej overhead)
    "spark.default.parallelism": "50",       # Zmniejszone z 100
    
    # Serialization - Use Kryo for better performance
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.kryo.registrationRequired": "false",
    "spark.kryoserializer.buffer.max": "512m",
    
    # Network timeouts
    "spark.network.timeout": "600s",         # Zwiększone z 300s
    "spark.executor.heartbeatInterval": "60s",
    
    # Adaptive Query Execution
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    
    # Cache settings
    "spark.sql.inMemoryColumnarStorage.compressed": "true",
    "spark.sql.inMemoryColumnarStorage.batchSize": "10000",
    
    # Broadcast join threshold (dla mniejszych joinów)
    "spark.sql.autoBroadcastJoinThreshold": "10m",
}

# Konfiguracja MongoDB
MONGODB_CONFIG = {
    "host": "mongodb",
    "port": 27017,
    "database": "nyc_taxi",
    "uri": "mongodb://mongodb:27017/"
}

# Ścieżki danych
DATA_PATHS = {
    "raw_data": "/home/jovyan/data/yellow_tripdata_2015-*.csv",
    "figures": "/home/jovyan/reports/figures",
    "reports": "/home/jovyan/reports/"
}

# Granice geograficzne NYC
NYC_BOUNDS = {
    "lon_min": -74.3,
    "lon_max": -73.7,
    "lat_min": 40.5,
    "lat_max": 40.95
}

# Mapowania
PAYMENT_TYPES = {
    1: "Credit Card",
    2: "Cash", 
    3: "No Charge",
    4: "Dispute"
}

DAY_NAMES = {
    1: "Niedziela",
    2: "Poniedziałek", 
    3: "Wtorek",
    4: "Środa",
    5: "Czwartek",
    6: "Piątek",
    7: "Sobota"
}

MONTH_NAMES = [
    'Sty', 'Lut', 'Mar', 'Kwi', 'Maj', 'Cze',
    'Lip', 'Sie', 'Wrz', 'Paź', 'Lis', 'Gru'
]

# ========== PARAMETRY MODELI ML (ZOPTYMALIZOWANE) ==========
ML_CONFIG = {
    "gbt_classifier": {
        "maxIter": 20,
        "maxDepth": 5,
        "seed": 42
    },
    "linear_regression": {
        "maxIter": 100,
        "regParam": 0.1,
        "elasticNetParam": 0.5
    },
    "train_test_split": 0.8,
    "seed": 42,
    
    # NOWE: Sampling dla dużych zbiorów
    "use_sampling": True,          
    "sample_fraction": 0.1,         
    "max_train_samples": 1000000     
}