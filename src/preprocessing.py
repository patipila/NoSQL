"""
Moduł do czyszczenia i przetwarzania danych
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, isnan, when, count
from pyspark.sql.types import DoubleType, FloatType
import pandas as pd
import sys
sys.path.append('/home/jovyan/work')

from config.spark_config import NYC_BOUNDS


class DataPreprocessor:
    """Klasa do czyszczenia i przetwarzania danych"""
    
    def __init__(self, nyc_bounds: dict = None):
        """
        Inicjalizacja preprocessora
        
        Args:
            nyc_bounds: Granice geograficzne NYC
        """
        self.nyc_bounds = nyc_bounds or NYC_BOUNDS
        self.cleaning_stats = {}
        
    def analyze_missing_values(self, df: DataFrame, sample_fraction: float = 0.1) -> pd.DataFrame:
        """
        Analizuje brakujące wartości w DataFrame
        
        Args:
            df: DataFrame do analizy
            sample_fraction: Frakcja próbki do analizy
            
        Returns:
            pd.DataFrame: Statystyki braków
        """
        print("Analiza brakujących wartości...")
        
        df_sample = df.sample(fraction=sample_fraction, seed=42)
        total = df_sample.count()
        
        missing_data = []
        for col_name in df.columns:
            dtype = df.schema[col_name].dataType
            
            if isinstance(dtype, (DoubleType, FloatType)):
                null_count = df_sample.filter(
                    col(col_name).isNull() | isnan(col(col_name))
                ).count()
            else:
                null_count = df_sample.filter(col(col_name).isNull()).count()
            
            missing_pct = (null_count / total) * 100
            missing_data.append({
                'column': col_name,
                'missing_count': null_count,
                'missing_pct': round(missing_pct, 2)
            })
        
        return pd.DataFrame(missing_data).sort_values('missing_pct', ascending=False)
    
    def clean_data(self, df: DataFrame) -> DataFrame:
        """
        Czyści dane według zdefiniowanych reguł
        
        Args:
            df: DataFrame do czyszczenia
            
        Returns:
            DataFrame: Oczyszczone dane
        """
        initial_count = df.count()
        print(f"\nCZYSZCZENIE DANYCH (początkowa liczba: {initial_count:,})")
        
        df_cleaned = df \
            .filter(col("tpep_pickup_datetime").isNotNull()) \
            .filter(col("tpep_dropoff_datetime").isNotNull()) \
            .filter(col("trip_distance") > 0) \
            .filter(col("trip_distance") < 100) \
            .filter(col("fare_amount") > 0) \
            .filter(col("fare_amount") < 500) \
            .filter(col("total_amount") > 0) \
            .filter(col("total_amount") < 500) \
            .filter(col("passenger_count") > 0) \
            .filter(col("passenger_count") <= 9) \
            .filter(col("tip_amount") >= 0) \
            .filter(col("tip_amount") < 200) \
            .filter(col("pickup_longitude").between(
                self.nyc_bounds['lon_min'], 
                self.nyc_bounds['lon_max']
            )) \
            .filter(col("pickup_latitude").between(
                self.nyc_bounds['lat_min'], 
                self.nyc_bounds['lat_max']
            )) \
            .filter(col("dropoff_longitude").between(
                self.nyc_bounds['lon_min'], 
                self.nyc_bounds['lon_max']
            )) \
            .filter(col("dropoff_latitude").between(
                self.nyc_bounds['lat_min'], 
                self.nyc_bounds['lat_max']
            )) \
            .filter(col("payment_type").isin([1, 2, 3, 4])) \
            .filter(col("tpep_dropoff_datetime") > col("tpep_pickup_datetime"))
        
        final_count = df_cleaned.count()
        removed_count = initial_count - final_count
        removed_pct = (removed_count / initial_count) * 100
        
        self.cleaning_stats = {
            'initial_count': initial_count,
            'final_count': final_count,
            'removed_count': removed_count,
            'removed_pct': removed_pct
        }
        
        print(f"  Przed: {initial_count:,}")
        print(f"  Po:    {final_count:,}")
        print(f"  Usunięto: {removed_count:,} ({removed_pct:.2f}%)")
        
        return df_cleaned
    
    def get_cleaning_stats(self) -> dict:
        """Zwraca statystyki czyszczenia"""
        return self.cleaning_stats