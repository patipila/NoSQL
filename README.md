# NYC Yellow Taxi Trip Data Analysis 2016

## Projekt zaliczeniowy - Systemy Big Data i NoSQL

### Opis
Analiza danych przejazdów taksówek w Nowym Jorku w roku 2016 z wykorzystaniem 
technologii Big Data (Apache Spark) oraz bazy NoSQL (MongoDB).

### Zbiór danych
- **Źródło:** [Kaggle - NYC Yellow Taxi Trip Data](https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data)
- **Rozmiar:** ~1.5 GB
- **Rekordy:** ~113 milionów przejazdów

### Technologie
- Apache Spark (PySpark)
- MongoDB
- Docker & Docker Compose
- Python, Pandas, Matplotlib, Seaborn

### Modele ML
1. **Random Forest** - klasyfikacja wysokich napiwków (>20%)
2. **Linear Regression** - predykcja całkowitej kwoty przejazdu

### Uruchomienie

```bash
# 1. Pobierz dane z Kaggle
kaggle datasets download -d elemento/nyc-yellow-taxi-trip-data -p data/
unzip data/nyc-yellow-taxi-trip-data.zip -d data/

# 2. Uruchom środowisko Docker
docker-compose up -d

# 3. Otwórz Jupyter Lab
# http://localhost:8888

# 4. Uruchom notebook main_analysis.ipynb
```