"""
Moduł do wczytywania danych NYC Taxi
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, IntegerType, 
    DoubleType, StringType, TimestampType
)
from datetime import datetime
import sys
sys.path.append('/home/jovyan/work')

from config.spark_config import SPARK_CONFIG, DATA_PATHS


class DataLoader:
    """Klasa do wczytywania i podstawowej walidacji danych"""
    
    def __init__(self, spark: SparkSession = None):
        """
        Inicjalizacja DataLoader
        
        Args:
            spark: Istniejąca sesja Spark lub None (zostanie utworzona nowa)
        """
        self.spark = spark or self._create_spark_session()
        self.schema = self._define_schema()
        
    def _create_spark_session(self) -> SparkSession:
        """Tworzy skonfigurowaną sesję Spark"""
        builder = SparkSession.builder.appName(SPARK_CONFIG["app_name"])
        
        # Dodanie konfiguracji
        for key, value in SPARK_CONFIG.items():
            if key not in ["app_name", "master"]:
                builder = builder.config(key, value)
        
        # Master URL
        if "master" in SPARK_CONFIG:
            builder = builder.master(SPARK_CONFIG["master"])
            
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        
        return spark
    
    def _define_schema(self) -> StructType:
        """Definiuje schemat danych dla optymalizacji wczytywania"""
        return StructType([
            StructField("VendorID", IntegerType(), True),
            StructField("tpep_pickup_datetime", TimestampType(), True),
            StructField("tpep_dropoff_datetime", TimestampType(), True),
            StructField("passenger_count", IntegerType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("pickup_longitude", DoubleType(), True),
            StructField("pickup_latitude", DoubleType(), True),
            StructField("RatecodeID", IntegerType(), True),
            StructField("store_and_fwd_flag", StringType(), True),
            StructField("dropoff_longitude", DoubleType(), True),
            StructField("dropoff_latitude", DoubleType(), True),
            StructField("payment_type", IntegerType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("extra", DoubleType(), True),
            StructField("mta_tax", DoubleType(), True),
            StructField("tip_amount", DoubleType(), True),
            StructField("tolls_amount", DoubleType(), True),
            StructField("improvement_surcharge", DoubleType(), True),
            StructField("total_amount", DoubleType(), True)
        ])
    
    def load_data(self, path: str = None, num_partitions: int = 200):
        """
        Wczytuje dane z plików CSV
        
        Args:
            path: Ścieżka do plików CSV (domyślnie z konfiguracji)
            num_partitions: Liczba partycji do repartycjonowania
            
        Returns:
            DataFrame: Wczytane dane
        """
        data_path = path or DATA_PATHS["raw_data"]
        
        print(f"Wczytywanie danych z: {data_path}")
        start_time = datetime.now()
        
        df = self.spark.read.csv(
            data_path,
            header=True,
            schema=self.schema,
            mode="DROPMALFORMED"
        )
        
        # Repartycjonowanie
        df = df.repartition(num_partitions)
        
        load_time = (datetime.now() - start_time).total_seconds()
        print(f"Czas wczytywania: {load_time:.2f} sekund")
        
        return df
    
    def get_data_info(self, df) -> dict:
        """
        Zwraca podstawowe informacje o zbiorze danych
        
        Args:
            df: DataFrame do analizy
            
        Returns:
            dict: Słownik z informacjami
        """
        info = {
            "record_count": df.count(),
            "column_count": len(df.columns),
            "columns": df.columns,
            "partitions": df.rdd.getNumPartitions(),
            "schema": df.schema
        }
        return info
    
    def print_data_info(self, df):
        """Wyświetla informacje o danych"""
        info = self.get_data_info(df)
        
        print("\n" + "=" * 70)
        print("INFORMACJE O ZBIORZE DANYCH")
        print("=" * 70)
        print(f"Liczba rekordów: {info['record_count']:,}")
        print(f"Liczba kolumn: {info['column_count']}")
        print(f"Liczba partycji: {info['partitions']}")
        print("\nKolumny:")
        for col in info['columns']:
            print(f"  - {col}")