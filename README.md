# NYC Yellow Taxi Trip Data Analysis 2016

## Projekt zaliczeniowy - Systemy Big Data i NoSQL

### Opis
Analiza danych przejazdów taksówek w Nowym Jorku w roku 2016 z wykorzystaniem
technologii Big Data (Apache Spark) oraz bazy NoSQL (MongoDB).

### Zbiór danych
- **Źródło:** [Kaggle - NYC Yellow Taxi Trip Data](https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data)
- **Rozmiar:** ~1.5 GB (skompresowane), ~11 GB (rozpakowane)
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
- `payment_type`: typ płatności (1=Karta, 2=Gotówka, 3=Bez opłaty, 4=Spór)
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
│  ┌────────────┐    ┌───────────────┐    ┌────────────────────────┐  │
│  │  CSV 2015  │──▶│    PySpark    │───▶│       MongoDB          │  │
│  │  (~1.5GB)  │    │   Processing  │    │  (agregacje/wyniki)    │  │
│  └────────────┘    └───────────────┘    └────────────────────────┘  │
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
├── docker-compose.yml           # Orkiestracja kontenerów
├── Dockerfile.jupyter           # Własny obraz Jupyter z Spark 3.5.3
├── README.md
├── .gitignore
│
├── config/
│   └── spark_config.py         # Konfiguracja Spark i MongoDB
│
├── src/
│   ├── __init__.py
│   ├── data_loader.py          # Wczytywanie danych
│   ├── preprocessing.py        # Czyszczenie danych
│   ├── features.py             # Inżynieria cech
│   └── models.py               # Modele ML
│
├── notebooks/
│   ├── 01_data_exploration.ipynb    # Wczytanie + EDA
│   ├── 02_preprocessing.ipynb       # Czyszczenie + MongoDB
│   └── 03_ml_modeling.ipynb         # ML + wyniki
│
├── data/
│   └── yellow_tripdata_2015-*.csv   # Dane (do pobrania)
│
└── reports/
    ├── raport_projekt.pdf
    └── figures/                     # Wykresy
        └── *.png
```

### Technologie
- **Apache Spark 3.5.3** (PySpark) - przetwarzanie rozproszonych danych
- **MongoDB 6.0** - baza NoSQL do przechowywania wyników
- **Docker & Docker Compose** - konteneryzacja
- **Python 3.11** - język programowania
- **Biblioteki:** Pandas, NumPy, Matplotlib, Seaborn, Scikit-learn

### Modele ML
1. **Random Forest** - klasyfikacja wysokich napiwków (>20%)
2. **Linear Regression** - predykcja całkowitej kwoty przejazdu

---

## Uruchomienie Projektu

### Wymagania
- **Docker Desktop** (Windows/Mac) lub **Docker Engine** (Linux)
- **Docker Compose** v2.0+
- **Minimum 8 GB RAM** (16 GB zalecane)
- **~15 GB wolnego miejsca** na dysku

### Krok 1: Sklonuj repozytorium
```bash
git clone <URL_REPOZYTORIUM>
cd nyc-taxi-analysis-2016
```

### Krok 2: Utwórz strukturę katalogów
```bash
# Windows (PowerShell)
New-Item -ItemType Directory -Force -Path data, reports/figures, notebooks

# Linux/Mac
mkdir -p data reports/figures notebooks
```

### Krok 3: Pobierz dane
Pobierz dane z Kaggle i umieść pliki CSV w folderze `data/`.

**Opcja A: Kaggle API (zalecane)**
```bash
# Zainstaluj Kaggle CLI
pip install kaggle

# Skonfiguruj API token (pobierz z https://www.kaggle.com/settings)
# Umieść kaggle.json w:
# - Windows: C:\Users\<username>\.kaggle\kaggle.json
# - Linux/Mac: ~/.kaggle/kaggle.json

# Pobierz dataset
kaggle datasets download -d elemento/nyc-yellow-taxi-trip-data

# Rozpakuj do folderu data/
# Windows
Expand-Archive -Path nyc-yellow-taxi-trip-data.zip -DestinationPath .\data\

# Linux/Mac
unzip nyc-yellow-taxi-trip-data.zip -d ./data/
```

**Opcja B: Ręczne pobranie**
1. Przejdź do: https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data
2. Kliknij "Download" (wymaga konta Kaggle)
3. Rozpakuj archiwum do folderu `data/`

**Upewnij się, że masz pliki:**
```
data/
├── yellow_tripdata_2015-01.csv
```

### Krok 4: Zbuduj i uruchom kontenery
```bash
# Zbuduj obraz Jupyter z dopasowaną wersją Spark
docker-compose build

# Uruchom wszystkie serwisy
docker-compose up -d

# Sprawdź status kontenerów
docker-compose ps
```

**Oczekiwany output:**
```
NAME                IMAGE                                           STATUS
jupyter             nyc-taxi-analysis-2016-jupyter                  Up
mongodb             mongo:6.0                                       Up
spark-master        apache/spark:3.5.3-scala2.12-java17-ubuntu     Up
spark-worker-1      apache/spark:3.5.3-scala2.12-java17-ubuntu     Up
spark-worker-2      apache/spark:3.5.3-scala2.12-java17-ubuntu     Up
```

### Krok 5: Sprawdź działanie klastra

**Spark Master UI:**
```
http://localhost:8080
```
Powinny być widoczne 2 workery w statusie "ALIVE".

**MongoDB:**
```bash
# Test połączenia
docker exec -it mongodb mongosh --eval "db.version()"
```

### Krok 6: Otwórz Jupyter Lab
```
open http://localhost:8888
```
> **Uwaga:** Token nie jest wymagany (wyłączony w konfiguracji)

### Krok 7: Uruchom notebooki
W JupyterLab otwórz i uruchom po kolei:

1. **`notebooks/01_data_exploration.ipynb`**
   - Wczytanie danych
   - Eksploracyjna analiza danych (EDA)
   - Podstawowe statystyki

2. **`notebooks/02_preprocessing.ipynb`**
   - Czyszczenie danych
   - Filtrowanie anomalii
   - Zapisywanie agregacji do MongoDB

3. **`notebooks/03_ml_modeling.ipynb`**
   - Trening modeli ML
   - Ewaluacja wyników
   - Generowanie wizualizacji

---

## Rozwiązywanie Problemów

### Problem: Kontenery nie startują
```bash
# Zatrzymaj wszystko i wyczyść
docker-compose down -v
docker system prune -f

# Zbuduj od nowa
docker-compose build --no-cache
docker-compose up -d
```

### Problem: Workery nie łączą się z Masterem
```bash
# Sprawdź logi
docker-compose logs spark-worker-1
docker-compose logs spark-master

# Powinno być: "Successfully registered with master"
```

### Problem: Błąd "InvalidClassException" podczas wczytywania danych
**Przyczyna:** Niezgodność wersji Spark między kontenerami.

**Rozwiązanie:** Sprawdź wersje:
```python
# W notebooku Jupyter
import pyspark
print(f"PySpark: {pyspark.__version__}")  # Powinno być 3.5.3

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
print(f"Spark: {spark.version}")  # Powinno być 3.5.3
```

### Problem: Brak pamięci (Out of Memory)
Zmniejsz zużycie pamięci w `docker-compose.yml`:
```yaml
environment:
  - SPARK_WORKER_MEMORY=512m  # Zamiast 1G
```

I w `config/spark_config.py`:
```python
"spark.executor.memory": "512m",
"spark.driver.memory": "512m",
```

### Problem: Jupyter nie widzi modułów (ImportError)
```python
# Na początku notebooka dodaj:
import sys
sys.path.append('/home/jovyan/src')
sys.path.append('/home/jovyan/config')
```

---

## Wizualizacje i Wyniki

Po wykonaniu wszystkich notebooków znajdziesz:

- **Wykresy:** `reports/figures/*.png`
- **Raport PDF:** `reports/raport_projekt.pdf`
- **Dane w MongoDB:** Kolekcje z agregacjami

### Dostęp do MongoDB
```bash
# Z lokalnego komputera
docker exec -it mongodb mongosh

# W shellu MongoDB
use nyc_taxi
show collections
db.daily_stats.find().limit(5)
```

---

## Zatrzymanie Projektu

```bash
# Zatrzymaj kontenery (dane pozostaną)
docker-compose stop

# Zatrzymaj i usuń kontenery (MongoDB data zostanie)
docker-compose down

# Usuń WSZYSTKO włącznie z danymi MongoDB
docker-compose down -v
```

---

## Przydatne Linki

- **Jupyter Lab:** http://localhost:8888
- **Spark Master UI:** http://localhost:8080
- **Spark Application UI:** http://localhost:4040 (gdy job działa)
- **MongoDB:** localhost:27017

---

## Autor

Projekt zaliczeniowy na przedmiot "Systemy Big Data i NoSQL"
Autorzy:

* Patrycja Piła
* Mateusz Strojek
* Magdalena Wnuk

---

## Licencja

Ten projekt wykorzystuje dane z NYC Taxi & Limousine Commission udostępnione na licencji publicznej.