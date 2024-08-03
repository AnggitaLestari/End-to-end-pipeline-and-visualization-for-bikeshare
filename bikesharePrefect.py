from prefect import flow, task
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from google.cloud import bigquery 
from google.oauth2 import service_account

@task
def create_db_engine():
    return create_engine('postgresql://postgres:(password)@localhost:1234/bikeshare2')

@task
def load_csv_to_postgresql(file_path: str, table_name: str, engine):
    df = pd.read_csv(file_path)
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    return f"Data dari {file_path} berhasil dimuat ke tabel {table_name}"

@task
def process_regions(df):
    return df[df['region_id'].isin([3, 5, 12, 13, 14])]

@task
def process_station_info(df):
    df = df.dropna(axis=1, how='all')
    if 'region_id' in df.columns:
        df = df.dropna(subset=['region_id'])
    null_percentages = df.isnull().mean() * 100
    threshold = 100
    columns_to_drop = null_percentages[null_percentages > threshold].index.tolist()
    return df.drop(columns=columns_to_drop)

@task
def process_trips(df):
    df = df.dropna(axis=1, how='all')
    null_percentages = df.isnull().mean() * 100
    threshold = 100
    columns_to_drop = null_percentages[null_percentages > threshold].index.tolist()
    df = df.drop(columns=columns_to_drop)
    
    df['start_date'] = pd.to_datetime(df['start_date'])
    df = df[(df['start_date'].dt.year == 2017) | (df['start_date'].dt.year == 2018)]
    
    fact_trips = df[['trip_id', 'duration_sec', 'start_date', 'start_station_id', 'end_date', 'end_station_id', 
                     'start_station_latitude', 'start_station_longitude', 'end_station_latitude', 'end_station_longitude', 
                     'start_station_geom', 'end_station_geom']]
    
    dims_trips_info = df[['trip_id', 'bike_number', 'subscriber_type', 
                          'member_birth_year', 'member_gender']]
    
    return fact_trips, dims_trips_info

@task
def create_dims_regions_station_info(engine):
    regions = pd.read_sql_table('regions', engine)
    station_info = pd.read_sql_table('station_info', engine)
    
    dims_regions_station_info = pd.merge(
        station_info,
        regions,
        on='region_id',
        how='left'
    )
    
    dims_regions_station_info = dims_regions_station_info.rename(columns={
        'name_x': 'station_name',
        'name_y': 'region_name'
    })
    
    cols = ['station_id', 'station_name', 'short_name', 'region_id', 'region_name',
            'capacity', 'has_kiosk', 'station_geom', 'lat', 'lon']
    dims_regions_station_info = dims_regions_station_info[
        [col for col in cols if col in dims_regions_station_info.columns]
    ]
    
    dims_regions_station_info.to_sql('dims_regions_station_info', engine, if_exists='replace', index=False)
    dims_regions_station_info.to_csv('dims_regions_station_info.csv', index=False)
    return "dims_regions_station_info created and saved"

@task
def drop_unnecessary_tables(engine):
    tables_to_drop = ['trips', 'station_info', 'regions']
    with engine.begin() as connection:
        connection.execute(text("SET CONSTRAINTS ALL DEFERRED"))
        drop_tables_sql = """
        DROP TABLE IF EXISTS trips CASCADE;
        DROP TABLE IF EXISTS station_info CASCADE;
        DROP TABLE IF EXISTS regions CASCADE;
        """
        connection.execute(text(drop_tables_sql))
        connection.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
    return "Unnecessary tables dropped"

@task
def verify_tables(engine, tables_to_drop):
    inspector = inspect(engine)
    remaining_tables = inspector.get_table_names()
    print("\nTabel-tabel yang tersedia di database setelah update:")
    for table in remaining_tables:
        print(f"- {table}")
    
    if all(table not in remaining_tables for table in tables_to_drop):
        print("\nProses update database selesai. Tabel trips, station_info, dan regions berhasil dihapus.")
    else:
        print("\nPeringatan: Beberapa tabel yang seharusnya dihapus masih ada di database.")
        print("Tabel yang seharusnya dihapus:", tables_to_drop)
        print("Tabel yang masih ada:", [table for table in tables_to_drop if table in remaining_tables])
    
    return remaining_tables

@task
def display_table_info(engine, table_name):
    print(f"\n--- Informasi untuk tabel {table_name} ---")
    
    df = pd.read_sql_table(table_name, engine)
    print("Daftar kolom:")
    print(df.columns.tolist())
    
    print("\nSample isian row:")
    print(df.head(3).to_string(index=False))

@task
def execute_query(engine, query):
    return pd.read_sql_query(query, engine)

@task
def upload_to_bigquery(df, table_id):
    credentials = service_account.Credentials.from_service_account_file('XXXXXXX.json')
    client = bigquery.Client(credentials=credentials)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    table = client.get_table(table_id)
    return f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}"

@task
def verify_initial_load(engine):
    tables = ['regions', 'station_info', 'trips']
    for table in tables:
        df = pd.read_sql_table(table, engine)
        print(f"\nVerifikasi tabel {table}:")
        print(f"Jumlah baris: {len(df)}")
        print(f"Kolom: {df.columns.tolist()}")
        print(f"Sample data:\n{df.head(3)}")

@task
def verify_processed_data(engine):
    tables = ['fact_trips', 'dims_trips_info', 'dims_regions_station_info']
    for table in tables:
        df = pd.read_sql_table(table, engine)
        print(f"\nVerifikasi tabel {table}:")
        print(f"Jumlah baris: {len(df)}")
        print(f"Kolom: {df.columns.tolist()}")
        print(f"Sample data:\n{df.head(3)}")

@task
def verify_query_results(results):
    for query_name, df in results.items():
        print(f"\nVerifikasi hasil query {query_name}:")
        print(f"Jumlah baris: {len(df)}")
        print(f"Kolom: {df.columns.tolist()}")
        print(f"Sample data:\n{df.head(3)}")

@flow
def bikeshare_etl():
    engine = create_db_engine()
    
    # Load CSV to PostgreSQL
    csv_files = [
        ('regions.csv', 'regions'),
        ('station_info.csv', 'station_info'),
        ('trips.csv', 'trips')
    ]
    for file_path, table_name in csv_files:
        load_csv_to_postgresql(file_path, table_name, engine)
    
    # Verify initial load
    verify_initial_load(engine)
    
    # ETL to star schema
    regions_df = pd.read_sql_table('regions', engine)
    processed_regions = process_regions(regions_df)
    processed_regions.to_sql('regions', engine, if_exists='replace', index=False)
    
    station_info_df = pd.read_sql_table('station_info', engine)
    processed_station_info = process_station_info(station_info_df)
    processed_station_info.to_sql('station_info', engine, if_exists='replace', index=False)
    
    trips_df = pd.read_sql_table('trips', engine)
    fact_trips, dims_trips_info = process_trips(trips_df)
    fact_trips.to_sql('fact_trips', engine, if_exists='replace', index=False)
    dims_trips_info.to_sql('dims_trips_info', engine, if_exists='replace', index=False)
    
    create_dims_regions_station_info(engine)
    
    # Verify processed data
    verify_processed_data(engine)
    
    tables_to_drop = ['trips', 'station_info', 'regions']
    drop_unnecessary_tables(engine)
    
    # Verify tables after dropping
    remaining_tables = verify_tables(engine, tables_to_drop)
    
    # Display table info
    tables_to_display = ['fact_trips', 'dims_trips_info', 'dims_regions_station_info']
    for table in tables_to_display:
        display_table_info(engine, table)
    
    print("\nProses verifikasi tabel selesai.")
    
    # Matrix to BigQuery
    queries = [
        ("Query 1", """
        SELECT
            dr.region_name AS regional_name,
            EXTRACT(YEAR FROM ft.start_date) AS year,
            COUNT(*) AS Total_Trips
        FROM fact_trips ft
        JOIN dims_regions_station_info dr ON ft.start_station_id = dr.station_id
        WHERE EXTRACT(YEAR FROM ft.start_date) BETWEEN 2017 AND 2018
        GROUP BY dr.region_name, EXTRACT(YEAR FROM ft.start_date)
        ORDER BY regional_name 
        """),
        ("Query 2", """
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
        """),
        ("Query 3", """
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
        """),
        ("Query 4", """
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
        """),
        ("Query 5", """
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
        """),
        ("Query 6", """
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
        """)
    ]
    
    results = {}
    for query_name, query in queries:
        results[query_name] = execute_query(engine, query)
    
    # Verify query results
    verify_query_results(results)
    
    table_ids = [
        "my-project-anggita.bikeshare.matrix-1",
        "my-project-anggita.bikeshare.matrix-2",
        "my-project-anggita.bikeshare.matrix-3",
        "my-project-anggita.bikeshare.matrix-4",
        "my-project-anggita.bikeshare.matrix-5",
        "my-project-anggita.bikeshare.matrix-6"
    ]
    
    for (query_name, df), table_id in zip(results.items(), table_ids):
        upload_result = upload_to_bigquery(df, table_id)
        print(f"{query_name}: {upload_result}")

if __name__ == "__main__":
    bikeshare_etl()