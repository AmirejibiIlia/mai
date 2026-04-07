import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "").replace("postgresql://", "postgresql+pg8000://", 1).replace("postgres://", "postgresql+pg8000://", 1)
engine = create_engine(DATABASE_URL)

csv_file = "sample_data_1.csv"  # Your CSV filename
table_name = "mastertable"

# Read CSV with semicolon separator
df = pd.read_csv(csv_file, sep=';')  # ADD sep=';'

print(f"📊 Uploading {len(df)} rows to '{table_name}'...")
print("\nFirst 5 rows:")
print(df.head())

# Upload to database
df.to_sql(
    table_name,
    engine,
    if_exists='replace',  # Options: 'replace', 'append', 'fail'
    index=False
)

print(f"\n✅ Done! Uploaded {len(df)} rows to table '{table_name}'")