import sys
import os
import pandas as pd
from sqlalchemy import create_engine
import dotenv

dotenv.load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_SERVER = os.getenv("DB_SERVER")
DB_PORT = os.getenv("DB_PORT", "1433")
DB_NAME = os.getenv("DB_NAME")

def main(csv_path):
    df = pd.read_csv(csv_path)

    engine = create_engine(
        f"mssql+pyodbc://{DB_USER}:{DB_PASSWORD}@{DB_SERVER}:{DB_PORT}/{DB_NAME}?driver=ODBC+Driver+17+for+SQL+Server"
    )

    table_name = 'my_table'
    df.to_sql(table_name, engine, index=False, if_exists='append')

    print("Data inserted successfully.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python fill_db.py <path_to_csv>")
        sys.exit(1)
    main(sys.argv[1])
