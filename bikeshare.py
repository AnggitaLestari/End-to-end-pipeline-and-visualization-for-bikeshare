#### Load CSV ke Postgresql

import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://postgres:(Password)@localhost:5432/bikeshare1')

def load_csv_to_postgresql(file_path, table_name):
    df = pd.read_csv(file_path)
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"Data dari {file_path} berhasil dimuat ke tabel {table_name}")

# Daftar file CSV dan nama tabel yang sesuai
csv_files = [
    ('regions.csv', 'regions'),
    ('station_info.csv', 'station_info'),
    ('trips.csv', 'trips')
]

# Memuat setiap file CSV ke tabel yang sesuai
for file_path, table_name in csv_files:
    load_csv_to_postgresql(file_path, table_name)

print("Semua data berhasil dimuat ke PostgreSQL")