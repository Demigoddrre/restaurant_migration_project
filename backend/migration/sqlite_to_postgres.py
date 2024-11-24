import sqlite3
import psycopg2
import json
import logging

# Set up logging
logging.basicConfig(filename='migration/logs/migration.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

# Load PostgreSQL connection details from config.json
with open('migration/config.json') as config_file:
    config = json.load(config_file)

# Function to migrate data from SQLite to PostgreSQL
def migrate_data():
    try:
        # Connect to SQLite3 database
        sqlite_conn = sqlite3.connect('data/restaurant.db')
        sqlite_cur = sqlite_conn.cursor()

        # Connect to PostgreSQL hosted on GCP
        pg_conn = psycopg2.connect(
            dbname=config['postgres']['dbname'],
            user=config['postgres']['user'],
            password=config['postgres']['password'],
            host=config['postgres']['host'],
            port=config['postgres']['port']
        )
        pg_cur = pg_conn.cursor()

        logging.info('Connected to both SQLite3 and PostgreSQL databases.')

        # Example: Migrating menu table from SQLite to PostgreSQL
        sqlite_cur.execute("SELECT * FROM menu")
        rows = sqlite_cur.fetchall()

        for row in rows:
            pg_cur.execute("""
                INSERT INTO menu (item_id, item_name, category, price, description, available)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, row)

        pg_conn.commit()
        logging.info(f'Migration of menu table completed. {len(rows)} rows migrated.')

    except Exception as e:
        logging.error(f'Error during migration: {e}')
    finally:
        # Close all connections
        if sqlite_conn:
            sqlite_conn.close()
        if pg_conn:
            pg_cur.close()
            pg_conn.close()
        logging.info('Connections closed.')

# Execute the migration
if __name__ == "__main__":
    migrate_data()
