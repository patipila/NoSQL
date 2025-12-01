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
import os

# Ścieżki do modułów
sys.path.append('/home/jovyan')
sys.path.append('/home/jovyan/src')
sys.path.append('/home/jovyan/config')

try:
    from config.spark_config import SPARK_CONFIG, DATA_PATHS
except ImportError:
    sys.path.append('..')
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
        print("Tworzenie sesji Spark...")
        
        # Stop existing session if any
        existing_spark = SparkSession.getActiveSession()
        if existing_spark:
            print("Zatrzymywanie istniejącej sesji Spark...")
            existing_spark.stop()
        
        # Build new session
        builder = SparkSession.builder.appName(SPARK_CONFIG["app_name"])
        
        # Apply all configurations
        for key, value in SPARK_CONFIG.items():
            if key not in ["app_name"]:
                builder = builder.config(key, str(value))
        
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        
        # Test connection
        print(f"Spark UI: http://localhost:4040")
        print(f"Master: {spark.sparkContext.master}")
        print(f"Spark Version: {spark.version}")
        print(f"Python Version: {sys.version}")
        
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
            path: Ścieżka do danych (opcjonalne)
            num_partitions: Liczba partycji do przetwarzania
            
        Returns:
            DataFrame: Wczytane dane
        """
        data_path = path or DATA_PATHS["raw_data"]

        print(f"\nWczytywanie danych z: {data_path}")
        
        # Check if files exist
        import glob
        files = glob.glob(data_path.replace('/home/jovyan/', './'))
        if not files:
            print(f"⚠️  UWAGA: Nie znaleziono plików pasujących do wzorca: {data_path}")
            print("Upewnij się, że pliki CSV znajdują się w folderze 'data/'")
        else:
            print(f"✓ Znaleziono {len(files)} plików")
        
        start_time = datetime.now()
        
        try:
            df = self.spark.read.csv(
                data_path,
                header=True,
                schema=self.schema,
                mode="DROPMALFORMED"
            )
            
            # Force evaluation to catch errors early
            count = df.count()
            
            if count == 0: 
                print("⚠️  UWAGA: Wczytany DataFrame jest pusty!")
                return df
            
            # Repartition for better parallelism
            df = df.repartition(num_partitions)
            
            load_time = (datetime.now() - start_time).total_seconds()
            print(f"✓ Wczytano {count:,} rekordów w {load_time:.2f} sekund")
            
            return df
            
        except Exception as e:
            print(f"\n❌ BŁĄD podczas wczytywania pliku:")
            print(f"   {str(e)}")
            print("\nSprawdź:")
            print("  1. Czy pliki CSV są w folderze 'data/'")
            print("  2. Czy Spark workers są uruchomione (docker ps)")
            print("  3. Czy wersje Spark są zgodne we wszystkich kontenerach")
            raise e
    
    def get_data_info(self, df) -> dict:
        """Pobiera informacje o DataFrame"""
        info = {
            "record_count": df.count(),
            "column_count": len(df.columns),
            "columns": df.columns,
            "partitions": df.rdd.getNumPartitions(),
            "schema": df.schema
        }
        return info
    
    def print_data_info(self, df):
        """Wyświetla informacje o DataFrame"""
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
        print("=" * 70)