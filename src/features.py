"""
Moduł do inżynierii cech
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, hour, dayofweek, month, dayofmonth,
    unix_timestamp
)


class FeatureEngineer:
    """Klasa do tworzenia nowych cech"""
    
    def __init__(self):
        """Inicjalizacja FeatureEngineer"""
        self.feature_columns = []
        
    def create_temporal_features(self, df: DataFrame) -> DataFrame:
        """
        Tworzy cechy czasowe
        
        Args:
            df: DataFrame z danymi
            
        Returns:
            DataFrame: Dane z nowymi cechami czasowymi
        """
        df = df \
            .withColumn("trip_duration_min",
                (unix_timestamp("tpep_dropoff_datetime") - 
                 unix_timestamp("tpep_pickup_datetime")) / 60) \
            .withColumn("pickup_hour", hour("tpep_pickup_datetime")) \
            .withColumn("pickup_day", dayofweek("tpep_pickup_datetime")) \
            .withColumn("pickup_month", month("tpep_pickup_datetime")) \
            .withColumn("pickup_dayofmonth", dayofmonth("tpep_pickup_datetime")) \
            .withColumn("is_weekend",
                when(dayofweek("tpep_pickup_datetime").isin([1, 7]), 1).otherwise(0)) \
            .withColumn("is_rush_hour",
                when(
                    ((hour("tpep_pickup_datetime") >= 7) & 
                     (hour("tpep_pickup_datetime") <= 9)) |
                    ((hour("tpep_pickup_datetime") >= 16) & 
                     (hour("tpep_pickup_datetime") <= 19)),
                    1
                ).otherwise(0)) \
            .withColumn("is_night",
                when(
                    (hour("tpep_pickup_datetime") >= 22) | 
                    (hour("tpep_pickup_datetime") <= 5),
                    1
                ).otherwise(0)) \
            .withColumn("time_of_day",
                when(col("pickup_hour").between(6, 11), "morning")
                .when(col("pickup_hour").between(12, 17), "afternoon")
                .when(col("pickup_hour").between(18, 21), "evening")
                .otherwise("night"))
        
        return df
    
    def create_trip_features(self, df: DataFrame) -> DataFrame:
        """
        Tworzy cechy związane z przejazdem
        
        Args:
            df: DataFrame z danymi
            
        Returns:
            DataFrame: Dane z nowymi cechami przejazdu
        """
        df = df \
            .withColumn("speed_mph",
                when(col("trip_duration_min") > 0,
                     col("trip_distance") / (col("trip_duration_min") / 60))
                .otherwise(0)) \
            .withColumn("distance_category",
                when(col("trip_distance") < 1, "very_short")
                .when(col("trip_distance") < 3, "short")
                .when(col("trip_distance") < 10, "medium")
                .otherwise("long"))
        
        return df
    
    def create_tip_features(self, df: DataFrame) -> DataFrame:
        """
        Tworzy cechy związane z napiwkami
        
        Args:
            df: DataFrame z danymi
            
        Returns:
            DataFrame: Dane z nowymi cechami napiwków
        """
        df = df \
            .withColumn("tip_percentage",
                when(col("fare_amount") > 0,
                     (col("tip_amount") / col("fare_amount")) * 100)
                .otherwise(0)) \
            .withColumn("high_tip",
                when(col("tip_percentage") > 20, 1).otherwise(0))
        
        return df
    
    def create_all_features(self, df: DataFrame) -> DataFrame:
        """
        Tworzy wszystkie cechy
        
        Args:
            df: DataFrame z danymi
            
        Returns:
            DataFrame: Dane ze wszystkimi nowymi cechami
        """
        print("\nTWORZENIE NOWYCH CECH...")
        
        df = self.create_temporal_features(df)
        df = self.create_trip_features(df)
        df = self.create_tip_features(df)
        
        # Filtrowanie nieprawidłowych wartości
        df = df \
            .filter(col("trip_duration_min") > 0) \
            .filter(col("trip_duration_min") < 180) \
            .filter(col("speed_mph") < 100) \
            .filter(col("speed_mph") > 0)
        
        print("Utworzono cechy:")
        print("  - Czasowe: pickup_hour, pickup_day, pickup_month, is_weekend, is_rush_hour, is_night")
        print("  - Przejazdu: trip_duration_min, speed_mph, distance_category")
        print("  - Napiwków: tip_percentage, high_tip")
        
        return df
    
    def get_numeric_features(self) -> list:
        """Zwraca listę cech numerycznych do modelowania"""
        return [
            "passenger_count",
            "trip_distance",
            "pickup_hour",
            "pickup_day",
            "pickup_month",
            "is_weekend",
            "is_rush_hour",
            "is_night"
        ]
    
    def get_categorical_features(self) -> list:
        """Zwraca listę cech kategorycznych"""
        return ["VendorID", "RatecodeID", "payment_type"]