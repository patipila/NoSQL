"""
Moduł do treningu i ewaluacji modeli ML
"""

from pyspark.sql import DataFrame
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
    RegressionEvaluator
)
from pyspark.ml import Pipeline, PipelineModel
from datetime import datetime
import pandas as pd
import sys
sys.path.append('/home/jovyan/work')

from config.spark_config import ML_CONFIG


class ModelTrainer:
    """Klasa do treningu i ewaluacji modeli ML"""
    
    def __init__(self):
        """Inicjalizacja ModelTrainer"""
        self.models = {}
        self.metrics = {}
        self.feature_importance = {}
        
    def prepare_features_pipeline(
        self, 
        numeric_features: list, 
        categorical_features: list
    ) -> list:
        """
        Przygotowuje pipeline do przetwarzania cech
        
        Args:
            numeric_features: Lista cech numerycznych
            categorical_features: Lista cech kategorycznych
            
        Returns:
            list: Lista etapów pipeline
        """
        stages = []
        
        # StringIndexer dla cech kategorycznych
        for col_name in categorical_features:
            indexer = StringIndexer(
                inputCol=str(col_name),
                outputCol=f"{col_name}_idx",
                handleInvalid="keep"
            )
            stages.append(indexer)
        
        # VectorAssembler
        assembler_inputs = numeric_features + [f"{col}_idx" for col in categorical_features]
        assembler = VectorAssembler(
            inputCols=assembler_inputs,
            outputCol="features_raw",
            handleInvalid="skip"
        )
        stages.append(assembler)
        
        # StandardScaler
        scaler = StandardScaler(
            inputCol="features_raw",
            outputCol="features",
            withStd=True,
            withMean=True
        )
        stages.append(scaler)
        
        self.assembler_inputs = assembler_inputs
        return stages
    
    def train_gbt_classifier(
        self,
        train_data: DataFrame,
        test_data: DataFrame,
        preprocessing_stages: list,
        label_col: str = "high_tip"
    ) -> dict:
        """
        Trenuje model GBT Classifier (Gradient Boosted Tree)
        
        Args:
            train_data: Dane treningowe
            test_data: Dane testowe
            preprocessing_stages: Etapy przetwarzania
            label_col: Kolumna etykiety
            
        Returns:
            dict: Wyniki modelu
        """
        print("\n" + "=" * 70)
        print("TRENING MODELU: GBT CLASSIFIER")
        print("=" * 70)
        
        # Konfiguracja modelu - zakładam, że konfiguracja dla GBT jest dostępna w ML_CONFIG
        # Jeśli nie, można użyć domyślnych wartości lub dodać do configu
        gbt_config = ML_CONFIG.get("gbt_classifier", {}) 
        
        # Inicjalizacja GBTClassifier
        # Możesz dostosować hiperparametry (maxIter, maxDepth itp.) 
        # w zależności od potrzeb lub pobrać je z configu
        gbt = GBTClassifier(
            labelCol=label_col,
            featuresCol="features",
            maxIter=gbt_config.get("maxIter", 20), # Domyślnie 20 iteracji
            maxDepth=gbt_config.get("maxDepth", 5), # Domyślna głębokość 5
            seed=gbt_config.get("seed", 42) # Domyślny seed
        )
        
        # Pipeline
        pipeline = Pipeline(stages=preprocessing_stages + [gbt])
        
        # Trening
        print("Trening modelu...")
        start_time = datetime.now()
        model = pipeline.fit(train_data)
        train_time = (datetime.now() - start_time).total_seconds()
        print(f"Czas treningu: {train_time:.2f} sekund")
        
        # Predykcja
        print("Wykonywanie predykcji...")
        predictions = model.transform(test_data)
        
        # Ewaluacja
        metrics = self._evaluate_classification(predictions, label_col)
        metrics["training_time"] = train_time
        
        # Feature importance
        gbt_model = model.stages[-1]
        importance = gbt_model.featureImportances.toArray()
        importance_df = pd.DataFrame({
            'feature': self.assembler_inputs,
            'importance': importance
        }).sort_values('importance', ascending=False)
        
        # Zapisanie wyników
        self.models["gbt_classifier"] = model
        self.metrics["gbt_classifier"] = metrics
        self.feature_importance["gbt_classifier"] = importance_df
        
        self._print_classification_metrics("GBT Classifier", metrics)
        
        return {
            "model": model,
            "predictions": predictions,
            "metrics": metrics,
            "feature_importance": importance_df
        }
    
    def train_linear_regression(
        self,
        train_data: DataFrame,
        test_data: DataFrame,
        preprocessing_stages: list,
        label_col: str = "total_amount"
    ) -> dict:
        """
        Trenuje model Linear Regression
        
        Args:
            train_data: Dane treningowe
            test_data: Dane testowe
            preprocessing_stages: Etapy przetwarzania
            label_col: Kolumna etykiety
            
        Returns:
            dict: Wyniki modelu
        """
        print("\n" + "=" * 70)
        print("TRENING MODELU: LINEAR REGRESSION")
        print("=" * 70)
        
        # Konfiguracja
        lr_config = ML_CONFIG["linear_regression"]
        lr = LinearRegression(
            labelCol=label_col,
            featuresCol="features",
            maxIter=lr_config["maxIter"],
            regParam=lr_config["regParam"],
            elasticNetParam=lr_config["elasticNetParam"]
        )
        
        # Pipeline
        pipeline = Pipeline(stages=preprocessing_stages + [lr])
        
        # Trening
        print("Trening modelu...")
        start_time = datetime.now()
        model = pipeline.fit(train_data)
        train_time = (datetime.now() - start_time).total_seconds()
        print(f"Czas treningu: {train_time:.2f} sekund")
        
        # Predykcja
        print("Wykonywanie predykcji...")
        predictions = model.transform(test_data)
        
        # Ewaluacja
        metrics = self._evaluate_regression(predictions, label_col)
        metrics["training_time"] = train_time
        
        # Współczynniki
        lr_model = model.stages[-1]
        coefficients = list(zip(self.assembler_inputs, lr_model.coefficients.toArray()))
        coef_df = pd.DataFrame(coefficients, columns=['feature', 'coefficient'])
        coef_df['abs_coef'] = coef_df['coefficient'].abs()
        coef_df = coef_df.sort_values('abs_coef', ascending=False)
        
        # Zapisanie
        self.models["linear_regression"] = model
        self.metrics["linear_regression"] = metrics
        self.feature_importance["linear_regression"] = coef_df
        
        self._print_regression_metrics("Linear Regression", metrics)
        
        return {
            "model": model,
            "predictions": predictions,
            "metrics": metrics,
            "coefficients": coef_df,
            "intercept": lr_model.intercept
        }
    
    def _evaluate_classification(self, predictions: DataFrame, label_col: str) -> dict:
        """Ewaluacja modelu klasyfikacji"""
        binary_eval = BinaryClassificationEvaluator(
            labelCol=label_col,
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC"
        )
        
        accuracy_eval = MulticlassClassificationEvaluator(
            labelCol=label_col, predictionCol="prediction", metricName="accuracy"
        )
        f1_eval = MulticlassClassificationEvaluator(
            labelCol=label_col, predictionCol="prediction", metricName="f1"
        )
        precision_eval = MulticlassClassificationEvaluator(
            labelCol=label_col, predictionCol="prediction", metricName="weightedPrecision"
        )
        recall_eval = MulticlassClassificationEvaluator(
            labelCol=label_col, predictionCol="prediction", metricName="weightedRecall"
        )
        
        return {
            "auc_roc": binary_eval.evaluate(predictions),
            "accuracy": accuracy_eval.evaluate(predictions),
            "f1_score": f1_eval.evaluate(predictions),
            "precision": precision_eval.evaluate(predictions),
            "recall": recall_eval.evaluate(predictions)
        }
    
    def _evaluate_regression(self, predictions: DataFrame, label_col: str) -> dict:
        """Ewaluacja modelu regresji"""
        rmse_eval = RegressionEvaluator(
            labelCol=label_col, predictionCol="prediction", metricName="rmse"
        )
        mae_eval = RegressionEvaluator(
            labelCol=label_col, predictionCol="prediction", metricName="mae"
        )
        r2_eval = RegressionEvaluator(
            labelCol=label_col, predictionCol="prediction", metricName="r2"
        )
        
        return {
            "rmse": rmse_eval.evaluate(predictions),
            "mae": mae_eval.evaluate(predictions),
            "r2": r2_eval.evaluate(predictions)
        }
    
    def _print_classification_metrics(self, model_name: str, metrics: dict):
        """Wyświetla metryki klasyfikacji"""
        print(f"\n{'─' * 40}")
        print(f"METRYKI {model_name.upper()}")
        print(f"{'─' * 40}")
        print(f"  AUC-ROC:    {metrics['auc_roc']:.4f}")
        print(f"  Accuracy:   {metrics['accuracy']:.4f}")
        print(f"  F1-Score:   {metrics['f1_score']:.4f}")
        print(f"  Precision:  {metrics['precision']:.4f}")
        print(f"  Recall:     {metrics['recall']:.4f}")
    
    def _print_regression_metrics(self, model_name: str, metrics: dict):
        """Wyświetla metryki regresji"""
        print(f"\n{'─' * 40}")
        print(f"METRYKI {model_name.upper()}")
        print(f"{'─' * 40}")
        print(f"  RMSE:  {metrics['rmse']:.4f}")
        print(f"  MAE:   {metrics['mae']:.4f}")
        print(f"  R²:    {metrics['r2']:.4f}")
    
    def get_summary(self) -> pd.DataFrame:
        """Zwraca podsumowanie wszystkich modeli"""
        summary_data = []
        
        if "gbt_classifier" in self.metrics:
            gbt = self.metrics["gbt_classifier"]
            summary_data.append({
                'Model': 'GBT Classifier',
                'Zadanie': 'Klasyfikacja',
                'Główna metryka': f"AUC: {gbt['auc_roc']:.4f}",
                'Dodatkowe': f"Acc: {gbt['accuracy']:.4f}, F1: {gbt['f1_score']:.4f}",
                'Czas (s)': f"{gbt['training_time']:.2f}"
            })
        
        if "linear_regression" in self.metrics:
            lr = self.metrics["linear_regression"]
            summary_data.append({
                'Model': 'Linear Regression',
                'Zadanie': 'Regresja',
                'Główna metryka': f"R²: {lr['r2']:.4f}",
                'Dodatkowe': f"RMSE: {lr['rmse']:.4f}, MAE: {lr['mae']:.4f}",
                'Czas (s)': f"{lr['training_time']:.2f}"
            })
        
        return pd.DataFrame(summary_data)