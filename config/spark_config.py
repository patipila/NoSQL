"""
Konfiguracja Apache Spark dla projektu NYC Taxi Analysis
"""

# Konfiguracja Spark Session
SPARK_CONFIG = {
    "app_name": "NYC_Taxi_Analysis_2016",
    "master": "spark://spark-master:7077",
    
    # Pamięćs
    "spark.executor.memory": "4g",
    "spark.driver.memory": "4g",
    "spark.executor.cores": "2",
    
    # Partycje i shuffle
    "spark.sql.shuffle.partitions": "200",
    "spark.default.parallelism": "100",
    
    # Optymalizacje
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    
    # Serializacja
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    
    # Cache
    "spark.sql.inMemoryColumnarStorage.compressed": "true",
    "spark.sql.inMemoryColumnarStorage.batchSize": "10000",
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
    "raw_data": "/home/jovyan/data/yellow_tripdata_2016-*.csv",
    "figures": "/home/jovyan/work/figures/",
    "reports": "/home/jovyan/work/reports/"
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

# Parametry modeli ML
ML_CONFIG = {
    "random_forest": {
        "numTrees": 100,
        "maxDepth": 10,
        "maxBins": 32,
        "seed": 42,
        "featureSubsetStrategy": "sqrt"
    },
    "linear_regression": {
        "maxIter": 100,
        "regParam": 0.1,
        "elasticNetParam": 0.5
    },
    "train_test_split": 0.8,
    "seed": 42
}