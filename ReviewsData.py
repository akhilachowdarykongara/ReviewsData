import sqlite3
import os
from supabase import create_client
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

# Supabase configuration
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

# Initialize Supabase client
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_sqlite_connection():
    """Create and return a connection to the SQLite database"""
    try:
        return sqlite3.connect('IMDB_Movies_2021.db')
    except sqlite3.Error as e:
        print(f"Error connecting to SQLite database: {e}")
        raise

def get_table_structure():
    """Get the structure of the REVIEWS table from SQLite"""
    try:
        conn = get_sqlite_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='REVIEWS';")
        create_statement = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return create_statement
    except sqlite3.Error as e:
        print(f"Error getting table structure: {e}")
        raise

def create_supabase_table(create_statement):
    """Check if the REVIEWS table exists in Supabase"""
    try:
        # Just check if table exists
        supabase.table('REVIEWS').select("*").limit(1).execute()
        print("Table exists in Supabase, proceeding with data load...")
    except Exception as e:
        print("Error: Please create the table in Supabase first using the SQL Editor")
        print("Use the following SQL:")
        print("""
       CREATE TABLE IF NOT EXISTS "REVIEWS" (
    "ID" SERIAL PRIMARY KEY,
    "REVIEW" TEXT,
    "RATING" NUMERIC,
    "AUTHOR" VARCHAR(255),
    "TITLE" TEXT
);
        """)
        raise

def get_existing_ids():
    """Get set of existing IDs from Supabase"""
    try:
        response = supabase.table('REVIEWS').select('ID').execute()
        return {record['ID'] for record in response.data}
    except Exception as e:
        print(f"Error fetching existing IDs from Supabase: {e}")
        return set()

def load_data(batch_size=100):
    """Load data from SQLite to Supabase in batches"""
    try:
        # Get existing IDs to avoid duplicates
        existing_ids = get_existing_ids()
        
        # Connect to SQLite
        sqlite_conn = get_sqlite_connection()
        cursor = sqlite_conn.cursor()
        
        # Get column names
        cursor.execute("PRAGMA table_info(REVIEWS)")
        columns = [col[1] for col in cursor.fetchall()]
        
        # Fetch all records
        cursor.execute("SELECT * FROM REVIEWS")
        
        records_processed = 0
        batch = []
        
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
                
            # Process each row
            for row in rows:
                record = dict(zip(columns, row))
                if record['ID'] not in existing_ids:
                    batch.append(record)
                    
                # Upload batch when it reaches batch_size
                if len(batch) >= batch_size:
                    try:
                        supabase.table('REVIEWS').insert(batch).execute()
                        records_processed += len(batch)
                        print(f"Processed {records_processed} records")
                        batch = []
                    except Exception as e:
                        print(f"Error uploading batch: {e}")
                        time.sleep(1)  # Wait before retrying
                        continue
        
        # Upload remaining records
        if batch:
            try:
                supabase.table('REVIEWS').insert(batch).execute()
                records_processed += len(batch)
                print(f"Processed {records_processed} records")
            except Exception as e:
                print(f"Error uploading final batch: {e}")
        
        cursor.close()
        sqlite_conn.close()
        
        print(f"Data loading completed. Total records processed: {records_processed}")
        
    except Exception as e:
        print(f"Error in data loading process: {e}")
        raise

def main():
    try:
        # Get table structure and create in Supabase if needed
        create_statement = get_table_structure()
        create_supabase_table(create_statement)
        
        # Load the data
        load_data()
        
    except Exception as e:
        print(f"Error in main process: {e}")

if __name__ == "__main__":
    main()