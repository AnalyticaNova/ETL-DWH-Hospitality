import numpy as np
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
from psycopg2.extras import execute_values
import subprocess
from datetime import datetime
import time
import os
from dotenv import load_dotenv  



# Dynamically get the base directory (parent directory of the script)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Construct dynamic paths
ENV_PATH = os.path.join(BASE_DIR, "hotel_dwh", ".env")
CSV_FILE_PATH = os.path.join(BASE_DIR, "hotel_dwh", "hotel_bookings.csv")

# Load the environment variables
load_dotenv(ENV_PATH)

# Securely fetch DB credentials
DB_NAME = "dbt_booking"
DB_USER = "postgres"
DB_PASS = os.getenv("DB_PASS")  
DB_HOST = "localhost"


def extract():
    """Extracts data from CSV and loads it into public.hotel_bookings."""
    print(f"Extracting data from {CSV_FILE_PATH}...")  

    # Load CSV into DataFrame
    df = pd.read_csv(CSV_FILE_PATH)  

    # Replace NaN values with None for PostgreSQL compatibility
    df = df.replace({np.nan: None})

    # Connect to PostgreSQL
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)
    cur = conn.cursor()

    # Insert query 
    insert_query = """
        INSERT INTO public.hotel_bookings (
            booking_id, hotel, is_canceled, lead_time, 
            arrival_date_year, arrival_date_month, arrival_date_day_of_month, 
            stays_in_weekend_nights, stays_in_week_nights, adults, children, babies, 
            meal, country, market_segment, distribution_channel, 
            is_repeated_guest, previous_cancellations, previous_bookings_not_canceled, 
            reserved_room_type, assigned_room_type, booking_changes, deposit_type, 
            agent, days_in_waiting_list, customer_type, adr, 
            required_car_parking_spaces, total_of_special_requests, 
            reservation_status, reservation_status_date
        ) VALUES %s
        ON CONFLICT (booking_id) DO NOTHING
    """

    # Convert DataFrame rows to list of tuples
    records = [tuple(row) for row in df.to_numpy()]

    # Execute batch insert
    execute_values(cur, insert_query, records)

    # Commit and close connection
    conn.commit()
    cur.close()
    conn.close()

    print(f"Extracted {len(df)} rows into public.hotel_bookings.")
    return f"Extracted {len(df)} records"


def load():
    """Triggers dbt run to refresh staging.stg_bookings."""
    print("Refreshing staging.stg_bookings using dbt...")

    try:
        # Run dbt to refresh staging tables
        subprocess.run(["dbt", "run"], cwd=DBT_PROJECT_PATH, check=True)  

        print("Staging tables updated successfully.")
        return "staging.stg_bookings refreshed via dbt."
    
    except subprocess.CalledProcessError as e:
        print(f"Error running dbt: {e}")
        return "dbt run failed!"

    

def transform():
    """Triggers dbt to process transformations and update marts tables."""
    print("Running dbt transformations...")

    result = subprocess.run(
        ["dbt", "run"], 
        cwd=DBT_PROJECT_PATH,  
        capture_output=True,
        text=True
    )

    if result.returncode == 0:
        print("DBT transformations completed successfully.")
        print(result.stdout)
        return "DBT Run Success"
    else:
        print("DBT transformation failed.")
        print(result.stderr)
        raise Exception("DBT Run Failed")



    

# Define default DAG arguments
default_args = {
    "owner": "neda",
    "depends_on_past": False,
    "start_date": datetime(2014, 4, 30),
    "retries": 1,
}


with DAG(
    "extract_load_transform",
    default_args=default_args,
    schedule_interval= None,
    catchup=False,  
    tags=["etl"],
) as dag:
    
    
    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract,
    )
    
    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load,
    )
    
    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform,
    )
    
    # Define task dependencies
    extract_task >> load_task >> transform_task
