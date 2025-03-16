import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv  

# Load environment variables from .env file
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  
ENV_PATH = os.path.join(BASE_DIR, "hotel_dwh", ".env")  
load_dotenv(ENV_PATH)

# Fetch database credentials from .env
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = "5432"  # Assuming port is always 5432
DB_NAME = os.getenv("DB_NAME")

# Construct dynamic CSV file path
CSV_FILE_PATH = os.getenv("CSV_FILE_PATH")

# Step 2: Connect to PostgreSQL using dynamic credentials
engine = create_engine(f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

# Step 3: Load CSV Data into Pandas
df = pd.read_csv(CSV_FILE_PATH)

# Step 4: Write Data to PostgreSQL
table_name = "raw_hotel_bookings"
df.to_sql(table_name, engine, if_exists="replace", index=False)

print(f"Data successfully loaded into PostgreSQL table: {table_name}")

