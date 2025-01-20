import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()
POSTGRES_URI = os.getenv("POSTGRES_URI")

source_engine = create_engine(POSTGRES_URI)

def extract_data(query, engine):
    try:
        data = pd.read_sql(query, engine)
        return data
    except Exception as e:
        print(f"Error during data extraction: {e}")
        raise

def transform_data(data):
    # data = data.drop(columns=["unwanted_column"])
    return data

def load_data_to_csv(data, file_path):
    try:
        data.to_csv(file_path, index=False)
    except Exception as e:
        print(f"Error during data saving: {e}")
        raise

def main():
    sql_query = "SELECT * FROM film ORDER BY film_id"

    output_csv_path = "output_data.csv"

    try:
        extracted_data = extract_data(sql_query, source_engine)
        transformed_data = transform_data(extracted_data)
        load_data_to_csv(transformed_data, output_csv_path)
    except Exception as e:
        print(f"ETL process failed: {e}")

if __name__ == "__main__":
    main()
