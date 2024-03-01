from typing import List
import requests
import psycopg2
import logging
from dataclasses import dataclass

@dataclass
class ForexData:
    currency_pair: str
    exchange_rate: float
    timestamp: str

class ForexDataFetcher:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = f"https://v6.exchangerate-api.com/v6/{self.api_key}/pair"

    def fetch_data(self, currency_pair: str) -> ForexData:
        url = f"{self.base_url}/{currency_pair}"
        response = requests.get(url)
        data = response.json()

        if response.status_code != 200 or data['result'] != 'success':
            raise Exception(f"Failed to fetch data from ExchangeRate-API: {data.get('error', 'Unknown error')}")

        exchange_rate = data['conversion_rate']
        timestamp = data['time_last_update_utc']

        return ForexData(currency_pair, exchange_rate, timestamp)
    
class DatabaseConnection:
    def __init__(self, db_name: str, user: str, password: str, host: str = "localhost", port: str = "5432"):
        logging.basicConfig(filename='database_writer.log', level=logging.ERROR)
        try:
            self.connection = psycopg2.connect(
                dbname=db_name,
                user=user,
                password=password,
                host=host,
                port=port
            )
            self.cursor = self.connection.cursor()
            print("connection successful")
            self.db_name = db_name
            self.create_database()
            self.create_forex_table()
        except (Exception, psycopg2.Error) as error:
            #logging.error("Error while connecting to PostgreSQL: %s", error)
            print("Error while connecting to PostgreSQL: %s", error)
    def create_database(self):
        try:
            # Check if the database exists
            self.cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{self.db_name}'")
            exists = self.cursor.fetchone()
            if not exists:
                # Create the database
                self.cursor.execute(f"CREATE DATABASE {self.db_name}")
                self.connection.commit()
                print(f"Database '{self.db_name}' created successfully.")
        except (Exception, psycopg2.Error) as error:
            logging.error("Error creating database: %s", error)
            print("Error creating database: %s", error)
    
    def create_forex_table(self):
        try:
            # Create the forex_data table if it doesn't exist
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS forex_data (
                    id SERIAL PRIMARY KEY,
                    currency_pair VARCHAR(10) NOT NULL,
                    exchange_rate NUMERIC,
                    timestamp TIMESTAMP
                )
            """)
            self.connection.commit()
            print("Table 'forex_data' query successful.")
        except (Exception, psycopg2.Error) as error:
            logging.error("Error creating table: %s", error)
            print("Error creating table: %s", error)

class DatabaseWriter():
    def __init__(self, dbconnection: DatabaseConnection):
        logging.basicConfig(filename='database_writer.log', level=logging.ERROR)
        try:
            self.dbname = dbconnection.db_name
            self.connection = dbconnection.connection
            self.cursor = dbconnection.cursor
        except (Exception, psycopg2.Error) as error:
            logging.error("Error while connecting to PostgreSQL: %s", error)
    
    def write_data(self, data: ForexData) -> None:
        try:
            insert_query = "INSERT INTO forex_data (currency_pair, exchange_rate, timestamp) VALUES (%s, %s, %s)"
            self.cursor.execute(insert_query, (data.currency_pair, data.exchange_rate, data.timestamp))
            print("insert query executed")
            self.connection.commit()
        except (Exception, psycopg2.Error) as error:
            logging.error("Error while inserting data into PostgreSQL table: %s", error)
            print(error)

    def __del__(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()

class ForexDataIngestionService:
    def __init__(self, data_fetcher: ForexDataFetcher, database_writer: DatabaseWriter) -> None:
        self.data_fetcher = data_fetcher
        self.database_writer = database_writer

    def fetch_data_from_exchange(self) -> ForexData:
        return self.data_fetcher.fetch_data()

    def write_data_to_database(self, data: ForexData) -> None:
        self.database_writer.write_data(data)

    def ingest_data(self) -> None:
        data = self.fetch_data_from_exchange()
        self.write_data_to_database(data)

if __name__ == "__main__":
    data_fetcher = ForexDataFetcher("9f372e5a2392105202948700")
    #database_writer = DatabaseWriter()
    #data_ingestion_service = ForexDataIngestionService(data_fetcher, database_writer)
    data = data_fetcher.fetch_data('USD/GBP')
    print(data_fetcher.fetch_data('USD/GBP'))
    db_conn = DatabaseConnection(db_name='postgres', user='postgres', password='postgres', host='prices_db')
    writer = DatabaseWriter(dbconnection=db_conn)
    writer.write_data(data)