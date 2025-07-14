import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine

# Load environment variables from .env file
load_dotenv()

def load_to_postgres(df, table_name):
    try:
        # Get DB credentials from environment variables
        db_user = os.getenv("DB_USER")
        db_pass = os.getenv("DB_PASSWORD")
        db_host = os.getenv("DB_HOST")
        db_port = os.getenv("DB_PORT")
        db_name = os.getenv("DB_NAME")

        # Debug: print to make sure they're not None
        print("Connecting as:", db_user, "@", db_host)

        # Create SQLAlchemy engine
        connection_string = f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(connection_string)

        # Load the DataFrame to PostgreSQL
        df.to_sql(table_name, engine, if_exists='replace', index=False)

        print(f"✅ Loaded data into table: {table_name}")
    except Exception as e:
        print(f"❌ Error loading data to PostgreSQL for table {table_name}: {e}")
