###### proses ETL to star skema #######

import pandas as pd
from sqlalchemy import create_engine, inspect, text

# Buat koneksi ke database PostgreSQL 'bikeshare2'
engine = create_engine('postgresql://postgres:(password)@localhost:5432/bikeshare2')

# Fungsi untuk memuat dan memproses data
def load_and_process_data(file_path, table_name, process_func=None):
    try:
        df = pd.read_csv(file_path)
        if process_func:
            df = process_func(df)
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Data dari {file_path} berhasil dimuat dan diproses ke tabel {table_name}")
    except Exception as e:
        print(f"Terjadi kesalahan saat memproses {file_path}: {str(e)}")

# Proses data regions
def process_regions(df):
    df = df[df['region_id'].isin([3, 5, 12, 13, 14])]
    return df

# Proses data station_info
def process_station_info(df):
    df = df.dropna(axis=1, how='all')
    if 'region_id' in df.columns:
        df = df.dropna(subset=['region_id'])
    null_percentages = df.isnull().mean() * 100
    threshold = 100
    columns_to_drop = null_percentages[null_percentages > threshold].index.tolist()
    df = df.drop(columns=columns_to_drop)
    return df

# Proses data trips
def process_trips(df):
    df = df.dropna(axis=1, how='all')
    null_percentages = df.isnull().mean() * 100
    threshold = 100
    columns_to_drop = null_percentages[null_percentages > threshold].index.tolist()
    df = df.drop(columns=columns_to_drop)
    
    # Filter data untuk tahun 2017 dan 2018
    df['start_date'] = pd.to_datetime(df['start_date'])
    df = df[(df['start_date'].dt.year == 2017) | (df['start_date'].dt.year == 2018)]
    
    # Memisahkan data menjadi fact_trips dan dims_trips_info
    fact_trips = df[['trip_id', 'duration_sec', 'start_date', 'start_station_id', 'end_date', 'end_station_id', 
                     'start_station_latitude', 'start_station_longitude', 'end_station_latitude', 'end_station_longitude', 
                     'start_station_geom', 'end_station_geom']]
    
    dims_trips_info = df[['trip_id', 'bike_number', 'subscriber_type', 
                          'member_birth_year', 'member_gender']]
    
    return fact_trips, dims_trips_info

# Memuat dan memproses data
load_and_process_data('regions.csv', 'regions', process_regions)
load_and_process_data('station_info.csv', 'station_info', process_station_info)

# Untuk trips, kita perlu menangani dua tabel yang dihasilkan
try:
    df_trips = pd.read_csv('trips.csv')
    fact_trips, dims_trips_info = process_trips(df_trips)
    fact_trips.to_sql('fact_trips', engine, if_exists='replace', index=False)
    dims_trips_info.to_sql('dims_trips_info', engine, if_exists='replace', index=False)
    print("Data trips berhasil dimuat dan diproses ke tabel fact_trips dan dims_trips_info")

# Simpan fact_trips dan dims_trips_info sebagai CSV
    fact_trips.to_csv('fact_trips.csv', index=False)
    dims_trips_info.to_csv('dims_trips_info.csv', index=False)
    print("fact_trips.csv dan dims_trips_info.csv berhasil disimpan")    
except Exception as e:
    print(f"Terjadi kesalahan saat memproses trips.csv: {str(e)}")

print("Semua data berhasil dimuat dan diproses ke PostgreSQL")

# Baca data dari tabel regions dan station_info
regions = pd.read_sql_table('regions', engine)
station_info = pd.read_sql_table('station_info', engine)

# Gabungkan tabel regions dengan station_info
dims_regions_station_info = pd.merge(
    station_info,
    regions,
    on='region_id',
    how='left'
)

# Ubah nama kolom
dims_regions_station_info = dims_regions_station_info.rename(columns={
    'name_x': 'station_name',
    'name_y': 'region_name'
})

# Pastikan urutan kolom sesuai dengan yang diinginkan
cols = ['station_id', 'station_name', 'short_name', 'region_id', 'region_name',
        'capacity', 'has_kiosk', 'station_geom', 'lat', 'lon']
dims_regions_station_info = dims_regions_station_info[
    [col for col in cols if col in dims_regions_station_info.columns]
]

# Simpan hasil gabungan ke tabel baru di database
dims_regions_station_info.to_sql('dims_regions_station_info', engine, if_exists='replace', index=False)

print("Tabel dims_regions_station_info berhasil dibuat dan disimpan ke database.")

# Simpan dims_regions_station_info sebagai CSV
dims_regions_station_info.to_csv('dims_regions_station_info.csv', index=False)
print("dims_regions_station_info.csv berhasil disimpan")

# Daftar tabel yang ingin dihapus
tables_to_drop = ['trips', 'station_info', 'regions']

# Hapus tabel yang tidak diperlukan
with engine.begin() as connection:
    try:
        # Nonaktifkan constraint sementara
        connection.execute(text("SET CONSTRAINTS ALL DEFERRED"))
        
        # Perintah SQL langsung untuk menghapus tabel
        drop_tables_sql = """
        DROP TABLE IF EXISTS trips CASCADE;
        DROP TABLE IF EXISTS station_info CASCADE;
        DROP TABLE IF EXISTS regions CASCADE;
        """
        
        connection.execute(text(drop_tables_sql))
        print("Perintah DROP TABLE berhasil dieksekusi.")
        
        # Aktifkan kembali constraint
        connection.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
        
        print("Proses penghapusan tabel selesai.")
    except Exception as e:
        print(f"Terjadi kesalahan saat menghapus tabel: {str(e)}")

# Verifikasi tabel-tabel yang tersisa di database
inspector = inspect(engine)
remaining_tables = inspector.get_table_names()

print("\nTabel-tabel yang tersedia di database setelah update:")
for table in remaining_tables:
    print(f"- {table}")

# Verifikasi bahwa tabel yang diinginkan sudah terhapus
if all(table not in remaining_tables for table in tables_to_drop):
    print("\nProses update database selesai. Tabel trips, station_info, dan regions berhasil dihapus.")
else:
    print("\nPeringatan: Beberapa tabel yang seharusnya dihapus masih ada di database.")
    print("Tabel yang seharusnya dihapus:", tables_to_drop)
    print("Tabel yang masih ada:", [table for table in tables_to_drop if table in remaining_tables])

# Fungsi untuk menampilkan informasi tabel
def display_table_info(table_name):
    print(f"\n--- Informasi untuk tabel {table_name} ---")
    
    # Tampilkan daftar kolom
    df = pd.read_sql_table(table_name, engine)
    print("Daftar kolom:")
    print(df.columns.tolist())
    
    # Tampilkan sample isian row
    print("\nSample isian row:")
    print(df.head(3).to_string(index=False))

# Daftar tabel yang ingin ditampilkan informasinya
tables_to_display = ['fact_trips', 'dims_trips_info', 'dims_regions_station_info']

# Tampilkan informasi untuk setiap tabel
for table in tables_to_display:
    display_table_info(table)

print("\nProses verifikasi tabel selesai.")    