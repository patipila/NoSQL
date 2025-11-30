# NYC Yellow Taxi Trip Data Analysis 2016

## Projekt zaliczeniowy - Systemy Big Data i NoSQL

### Opis
Analiza danych przejazdów taksówek w Nowym Jorku w roku 2016 z wykorzystaniem
technologii Big Data (Apache Spark) oraz bazy NoSQL (MongoDB).

### Zbiór danych
- **Źródło:** [Kaggle - NYC Yellow Taxi Trip Data](https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data)
- **Rozmiar:** ~1.5 GB
- **Rekordy:** ~113 milionów przejazdów
- **Format:** CSV

**Atrybuty:**
- `VendorID`: identyfikator dostawcy
- `tpep_pickup_datetime`: data i czas rozpoczęcia
- `tpep_dropoff_datetime`: data i czas zakończenia
- `passenger_count`: liczba pasażerów
- `trip_distance`: dystans (mile)
- `pickup_longitude/latitude`: współrzędne startu
- `dropoff_longitude/latitude`: współrzędne końca
- `RatecodeID`: kod taryfy
- `store_and_fwd_flag`: flaga store and forward
- `payment_type`: typ płatności
- `fare_amount`: opłata podstawowa
- `extra`: dodatkowe opłaty
- `mta_tax`: podatek MTA
- `tip_amount`: napiwek
- `tolls_amount`: opłaty drogowe
- `improvement_surcharge`: dopłata
- `total_amount`: całkowita kwota

### Architektura Projektu
```
┌─────────────────────────────────────────────────────────────────────┐
│                      ARCHITEKTURA PROJEKTU                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌────────────┐    ┌───────────────┐    ┌────────────────────────┐ │
│  │  CSV 2016  │───▶│    PySpark    │───▶│       MongoDB          │ │
│  │  (~1.5GB)  │    │   Processing  │    │  (agregacje/wyniki)    │ │
│  └────────────┘    └───────────────┘    └────────────────────────┘ │
│                           │                        │                │
│                           ▼                        ▼                │
│                 ┌─────────────────────────────────────┐             │
│                 │          Spark MLlib                │             │
│                 │  • Random Forest (klasyfikacja)     │             │
│                 │  • Linear Regression (regresja)     │             │
│                 └─────────────────────────────────────┘             │
│                                │                                    │
│                                ▼                                    │
│                 ┌─────────────────────────────────────┐             │
│                 │     Wizualizacje + Raport PDF       │             │
│                 └─────────────────────────────────────┘             │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Struktura Projektu
```
nyc-taxi-analysis-2016/
│
├── docker-compose.yml
├── README.md
├── requirements.txt
├── .gitignore
│
├── config/
│   └── spark_config.py
│
├── src/
│   ├── __init__.py
│   ├── data_loader.py
│   ├── preprocessing.py
│   ├── features.py
│   └── models.py
│
├── notebooks/
│   ├── 01_data_exploration.ipynb    # Wczytanie + EDA
│   ├── 02_preprocessing.ipynb       # Czyszczenie + MongoDB
│   └── 03_ml_modeling.ipynb         # ML + wyniki
│
├── data/
│   └── yellow_tripdata_2016-*.csv   # (nie commitować)
│
├── reports/
│   └── raport_projekt.pdf
│
└── figures/
    └── *.png                        # Wykresy
```

### Technologie
- Apache Spark (PySpark)
- MongoDB
- Docker & Docker Compose
- Python, Pandas, Matplotlib, Seaborn

### Modele ML
1. **Random Forest** - klasyfikacja wysokich napiwków (>20%)
2. **Linear Regression** - predykcja całkowitej kwoty przejazdu

### Uruchomienie

1.  **Pobranie danych**
    ```bash
    # Utworzenie katalogu na dane
    mkdir -p data
    cd data

    # Pobranie danych z Kaggle (wymagane Kaggle API)
    kaggle datasets download -d elemento/nyc-yellow-taxi-trip-data
    unzip nyc-yellow-taxi-trip-data.zip
    cd ..
    ```

2.  **Uruchomienie kontenerów**
    ```bash
    docker-compose up -d
    ```

3.  **Sprawdzenie logów (opcjonalnie)**
    ```bash
    docker-compose logs -f jupyter
    ```

4.  **Otwarcie Jupyter Lab**
    Przejdź do `http://localhost:8888` w przeglądarce.

5.  **Uruchomienie notebooków**
    Otwórz i uruchom po kolei notebooki w folderze `notebooks/`.
