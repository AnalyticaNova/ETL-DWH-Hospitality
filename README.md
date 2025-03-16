# ETL Pipeline for Data Warehouse

### Automating Data Extraction, Transformation, and Loading Using **Airflow, dbt, and PostgreSQL**

<br><br>

## Project Overview

This project is a fully **automated ETL pipeline** that processes hotel booking data from a CSV file, loads it into a **PostgreSQL database**, transforms it using **dbt**, and schedules the workflow using **Apache Airflow**. The goal is to create a **structured data warehouse** that can be used for **analytics and business insights.**

- **End-to-End ETL Automation:** Extracts raw data, loads it into a database, and applies transformations to create meaningful insights.
- **Data Stack:** Uses **Apache Airflow, dbt, and PostgreSQL** for scalable, reliable **data processing.**
- **Reproducible & Modular:** The pipeline is designed for easy scheduling, monitoring, and maintenance.
- **SQL & Python Integration:** Combines SQL transformations with **Python-based automation.**

### Project Objectives

- Extract hotel booking data from a CSV file
- Load the raw data into PostgreSQL for **structured storage**
- Apply **data transformations** using **dbt** to create **staging & fact tables**
- Automate the workflow with **Apache Airflow**
- Provide a **structured data warehouse** for **business analytics**

### Project Structure

The project is organized into a single Git repository with the following structure:

```
etl_project/
│── airflow/                # Apache Airflow DAGs & Configuration
│   ├── dags/               # Python scripts for ETL pipeline
│   │   ├── extract_load_transform.py
│   ├── logs/               # Airflow logs (.gitignore)
│   ├── .env                # Airflow environment variables (.gitignore)
│── hotel_dwh/              # dbt Project for Transformations
│   ├── models/             # SQL models for staging and marts
│   │   ├── staging/        # Raw data transformations
│   │   ├── marts/          # Business-ready tables
│   ├── dbt_project.yml     # dbt project configuration
│   ├── .env                # Database credentials (.gitignore)
│── .gitignore              # Ignoring logs, sensitive files, and compiled artifacts
│── requirements.txt        # Python dependencies for the project
│── README.md               # Project documentation (this file)
```

### Technologies & Tools Used

- Apache Airflow: Automates ETL workflow and scheduling
- PostgreSQL: Stores structured hotel booking data
- dbt: Performs SQL transformations in staging & marts tables
- Python: Automates the ETL process with scripts
- pandas: andles CSV data loading and manipulation
- SQLAlchemy: Connects Python to PostgreSQL database
- psycopg2: Executes SQL queries from Python
- dotenv: Manages credentials securely using environment variables
- GitHub: Version control for the ETL project


### ETL Pipeline Workflow

#### - **Extraction**

The extract() function in **Airflow DAG:**
- Reads raw hotel_bookings.csv from the project folder.
- **Cleans and prepares data** for insertion.
- Loads it into PostgreSQL (public.hotel_bookings).

#### - **Loading**

The load() function:
- Uses dbt to transform raw data into **structured staging tables** (staging.stg_bookings).
- Ensures **data integrity** before running transformations.

#### - **Transformation**

The transform() function:
- Runs **dbt transformations** to create:
- staging.stg_bookings: Cleans raw hotel booking data.
- marts.fact_bookings: **Business-ready booking data.**
- marts.dim_hotels: Hotel dimension table.
- marts.dim_customers: **Customer segmentation data.**

The data is now ready for analysis and reporting.

#### - **Automation & Scheduling** 

**Apache Airflow DAG (extract_load_transform.py):**
- Automates the ETL pipeline daily (@daily).
- Can be manually triggered using:

```
airflow dags trigger extract_load_transform
```

The pipeline ensures fresh, accurate data for analysis.


### Data Models & Schema

- **Staging Table:** staging.stg_bookings
- **Fact Table**: marts.fact_bookings
- **Dimension Tables**: marts.dim_hotels & marts.dim_customers	


<br><br>

## How to Run the Project

#### 1️⃣ Clone the Repository

```
git clone https://github.com/your-username/ETL-DWH-Hospitality.git
cd etl_project
```

#### 2️⃣ Set Up a Virtual Environment

```
python3 -m venv venv
source venv/bin/activate  # macOS/Linux
venv\Scripts\activate  # Windows
```

#### 3️⃣ Install Dependencies

```
pip install -r requirements.txt
```

#### 4️⃣ Set Up the Database
Ensure PostgreSQL is running and create the database:

```
CREATE DATABASE dbt_booking;
```

Load initial data:

```
python hotel_dwh/load_to_postgres.py
```

#### 5️⃣ Configure Environment Variables
Create a .env file inside hotel_dwh with:


```
DB_PASS="your_password"
DB_NAME=dbt_booking
DB_USER=postgres
DB_HOST=localhost
CSV_FILE_PATH=/absolute/path/to/hotel_bookings.csv 
```

#### 6️⃣ Run dbt Transformations

```
cd hotel_dwh
dbt run
```

#### 7️⃣ Start Apache Airflow

```
airflow webserver --port 8080 &
airflow scheduler &
```

Open Airflow UI at http://localhost:8080

Trigger the DAG extract_load_transform.


<br><br>


## 📌 Key Takeaways

- **End-to-End ETL pipeline using Python, SQL, and dbt**
  
- **Automated scheduling with Apache Airflow**

- **Efficient data processing with PostgreSQL**

- **Scalable & reproducible for real-world analytics**




### 📩 Connect With Me
If you're interested in discussing this project or opportunities, feel free to connect! 🚀

💼 LinkedIn: [https://www.linkedin.com/in/nedaetebari/]