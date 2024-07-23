import pandas as pd
from sqlalchemy import create_engine, inspect, text
from google.cloud import bigquery 
from google.oauth2 import service_account

# PostgreSQL connection
engine = create_engine('postgresql://postgres:(Password)@localhost:5432/bikeshare2')

### 1. Jumlah Pemakaian Sewa Sepeda per tahun per wilayah

query_satu = """
SELECT
    dr.region_name AS regional_name,
    EXTRACT(YEAR FROM ft.start_date) AS year,
    COUNT(*) AS Total_Trips
FROM fact_trips ft
JOIN dims_regions_station_info dr ON ft.start_station_id = dr.station_id
WHERE EXTRACT(YEAR FROM ft.start_date) BETWEEN 2017 AND 2018
GROUP BY dr.region_name, EXTRACT(YEAR FROM ft.start_date)
ORDER BY regional_name 
"""

df_matriksatu = pd.read_sql_query(query_satu, engine)
print("Query 1 Results:")
print(df_matriksatu.columns.tolist())
print(df_matriksatu.head(3).to_string(index=False))
print("\n")

### 2. Rata - Rata Durasi Perjalanan Berdasarkan Wilayah, Tahun, dan Jenis Member (subcriber/customer)

query_dua = """
SELECT
  dr.region_name AS Region,
  EXTRACT(YEAR FROM ft.start_date) AS Year,
  ROUND(AVG(ft.duration_sec) / 60,2) AS Average_Duration_Minutes,
  dt.subscriber_type AS Member_type
FROM fact_trips ft
JOIN dims_regions_station_info dr ON ft.start_station_id = dr.station_id
JOIN dims_trips_info dt ON ft.trip_id = dt.trip_id
WHERE EXTRACT(YEAR FROM ft.start_date) IN (2017, 2018)
GROUP BY dr.region_name, EXTRACT(YEAR FROM ft.start_date), Member_type, dt.subscriber_type
ORDER BY Region DESC, Member_type
"""
df_matrikdua = pd.read_sql_query(query_dua, engine)
print("Query 2 Results:")
print(df_matrikdua.columns.tolist())
print(df_matrikdua.head(3).to_string(index=False))
print("\n")

### 3. Nama Stasiun Awal dan Stasiun Akhir dengan Jumlah Perjalanan Terbanyak dan berada di wilayah region mana stasiun tersebut

query_tiga = """
SELECT
    dr1.station_name AS start_station_name,   
    dr1.region_name AS start_region,
    dr2.station_name AS end_station_name,
    dr2.region_name AS end_region,
    COUNT(ft.trip_id) AS total_trips
FROM fact_trips ft
JOIN dims_regions_station_info dr1 ON ft.start_station_id = dr1.station_id
JOIN dims_regions_station_info dr2 ON ft.end_station_id = dr2.station_id
JOIN dims_trips_info dt ON ft.trip_id = dt.trip_id
GROUP BY dr1.region_name,dr2.region_name,start_station_name,end_station_name 
ORDER BY total_trips desc
LIMIT 10
"""
df_matriktiga = pd.read_sql_query(query_tiga, engine)
print("Query 3 Results:")
print(df_matriktiga.columns.tolist())
print(df_matriktiga.head(3).to_string(index=False))
print("\n")

### 4. Analisis Pemanfaatan Stasiun Sepeda per Regional 

query_empat = """
WITH station_stats AS (
    SELECT 
        dr.region_name,
        dr.station_id,
        dr.station_name,
        dr.capacity,
        COUNT(ft.trip_id) as total_trips,
        30 as days_in_period, -- Asumsikan periode 30 hari
        CASE 
            WHEN dr.capacity > 0 THEN   
                (COUNT(ft.trip_id)::float / dr.capacity) / (30::float / 7)
            ELSE 0
        END AS trips_per_bike_per_week,
        CASE 
            WHEN dr.capacity > 0 THEN
                CASE
                    WHEN (COUNT(ft.trip_id)::float / dr.capacity) / (30::float / 7) < 7 THEN 'Underutilized'
                    WHEN (COUNT(ft.trip_id)::float / dr.capacity) / (30::float / 7) <= 14 THEN 'Ideal'
                    ELSE 'Overutilized'
                END
            ELSE 'No data'
        END AS utilization_status
    FROM 
        dims_regions_station_info dr
    LEFT JOIN 
        fact_trips ft ON dr.station_id = ft.start_station_id
    WHERE 
        dr.capacity > 0
    GROUP BY 
        dr.region_name, dr.station_id, dr.station_name, dr.capacity
),
region_totals AS (
    SELECT 
        region_name, 
        COUNT(station_id) as total_regional_stations
    FROM 
        station_stats
    GROUP BY 
        region_name
)
SELECT
    ss.region_name,
    ss.utilization_status,
    COUNT(ss.station_id) as total_stations,
    SUM(ss.total_trips) as total_trips,
    SUM(ss.capacity) as total_capacity,
    ROUND(AVG(ss.trips_per_bike_per_week)::numeric, 0) as avg_trips_per_bike_per_week,
    ROUND((COUNT(ss.station_id)::float / rt.total_regional_stations * 100)::numeric, 0) as percentage_of_stations
FROM 
    station_stats ss
JOIN
    region_totals rt ON ss.region_name = rt.region_name
GROUP BY
    ss.region_name, ss.utilization_status, rt.total_regional_stations
ORDER BY
    ss.region_name,
    CASE ss.utilization_status
        WHEN 'Underutilized' THEN 1
        WHEN 'Ideal' THEN 2
        WHEN 'Overutilized' THEN 3
        ELSE 4
    END
"""
df_matrikempat = pd.read_sql_query(query_empat, engine)
print("Query 4 Results:")
print(df_matrikempat.columns.tolist())
print(df_matrikempat.head(3).to_string(index=False))
print("\n")

### 5. Distribusi Trip Berdasarkan Wilayah, Tahun, Gender, dan Kelompok Usia

query_lima = """
SELECT
  dr.region_name AS Region,
  EXTRACT(YEAR FROM ft.start_date) AS Year,
  COUNT(*) AS Total_Trips,
  SUM(CASE WHEN dt.member_gender = 'Male' THEN 1 ELSE 0 END) AS Male_Trips,
  SUM(CASE WHEN dt.member_gender = 'Female' THEN 1 ELSE 0 END) AS Female_Trips,
  CASE
    WHEN EXTRACT(YEAR FROM CURRENT_DATE) - dt.member_birth_year BETWEEN 18 AND 39 THEN 'Young Adults'
    WHEN EXTRACT(YEAR FROM CURRENT_DATE) - dt.member_birth_year BETWEEN 40 AND 64 THEN 'Middle-aged Adults'
    WHEN EXTRACT(YEAR FROM CURRENT_DATE) - dt.member_birth_year >= 65 THEN 'Senior Adults'
    ELSE 'Unidentified'
  END AS Age_Group
FROM fact_trips ft
JOIN dims_regions_station_info dr ON ft.start_station_id = dr.station_id
JOIN dims_trips_info dt ON ft.trip_id = dt.trip_id
WHERE EXTRACT(YEAR FROM ft.start_date) IN (2017, 2018)
  AND dt.member_gender IN ('Male', 'Female')
  AND dt.member_birth_year IS NOT NULL
GROUP BY dr.region_name, EXTRACT(YEAR FROM ft.start_date), Age_Group
ORDER BY Region DESC, Age_Group
"""
df_matriklima = pd.read_sql_query(query_lima, engine)
print("Query 5 Results:")
print(df_matriklima.columns.tolist())
print(df_matriklima.head(3).to_string(index=False))
print("\n")

### 6. Hari dan Jam Tersibuk per Wilayah dan Tahun (2017-2018)

query_enam = """
WITH regional_peaks AS (
    SELECT
        dr.region_name,
        EXTRACT(YEAR FROM ft.start_date) AS year,
        CASE EXTRACT(DOW FROM ft.start_date)
            WHEN 0 THEN 'Sunday'
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
        END AS day_of_week,
        EXTRACT(HOUR FROM ft.start_date) AS hour_of_day,
        COUNT(*) AS total_trips,
        ROW_NUMBER() OVER (PARTITION BY dr.region_name, EXTRACT(YEAR FROM ft.start_date) ORDER BY COUNT(*) DESC) AS rank
    FROM fact_trips ft
    JOIN dims_regions_station_info dr ON ft.start_station_id = dr.station_id
    WHERE EXTRACT(YEAR FROM ft.start_date) IN (2017, 2018)
    GROUP BY dr.region_name, year, day_of_week, hour_of_day
)
SELECT 
    region_name,
    year,
    day_of_week,
    hour_of_day,
    total_trips
FROM regional_peaks
WHERE rank = 1
ORDER BY region_name, year, total_trips DESC
"""
df_matrikenam = pd.read_sql_query(query_enam, engine)
print("Query 6 Results:")
print(df_matrikenam.columns.tolist())
print(df_matrikenam.head(3).to_string(index=False))
print("\n")

# BigQuery upload
credentialsPath = r'XXXXXXXXX.json'

credentials = service_account.Credentials.from_service_account_file(credentialsPath)
client = bigquery.Client(credentials=credentials)

job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
    autodetect = True
)

table_ids = [
    "my-project-anggita.bikeshare.matrix-1",
    "my-project-anggita.bikeshare.matrix-2",
    "my-project-anggita.bikeshare.matrix-3",
    "my-project-anggita.bikeshare.matrix-4",
    "my-project-anggita.bikeshare.matrix-5",
    "my-project-anggita.bikeshare.matrix-6"
]

dataframes = [df_matriksatu, df_matrikdua, df_matriktiga, df_matrikempat, df_matriklima, df_matrikenam]

for i, (df, table_id) in enumerate(zip(dataframes, table_ids), 1):
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")