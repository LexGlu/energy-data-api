import psycopg2
import pandas as pd
import os

def create_connection():
    connection = psycopg2.connect(
        # host='db',
        # port='5432',
        # user='postgres',
        # password='password',
        # database='postgres'
        host=os.environ.get('POSTGRES_HOST'),
        port=os.environ.get('POSTGRES_PORT'),
        user=os.environ.get('POSTGRES_USER'),
        password=os.environ.get('POSTGRES_PASSWORD'),
        database=os.environ.get('POSTGRES_DB')
    )
    return connection

def create_table():
    connection = create_connection()
    cursor = connection.cursor()
    query = """
        CREATE TABLE IF NOT EXISTS dam_data (
            date DATE,
            hour INTEGER,
            price NUMERIC,
            volume NUMERIC
        )
    """
    cursor.execute(query)
    connection.commit()
    cursor.close()
    connection.close()
    
def insert_data(date, hour, price, volume):
    connection = create_connection()
    cursor = connection.cursor()
    query = """
        INSERT INTO dam_data (date, hour, price, volume) VALUES (%s, %s, %s, %s)
    """
    cursor.execute(query, (date, hour, price, volume))
    connection.commit()
    cursor.close()
    connection.close()
    
def get_data_for_date(date):
    try:
        date = pd.to_datetime(date, format='%d.%m.%Y')
    except Exception as e:
        raise ValueError(f'Invalid date format for {date}: {e}\n Indicate date in format dd.mm.yyyy')
    
    connection = create_connection()
    cursor = connection.cursor()
    query = """
        SELECT * FROM dam_data WHERE date = %s
    """
    cursor.execute(query, (date,))
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=['date', 'hour', 'price', 'volume']).sort_values(by=['date', 'hour'])
    cursor.close()
    connection.close()
    return df

def get_data_for_date_range(start_date, end_date):
    try:
        start_date = pd.to_datetime(start_date, format='%d.%m.%Y')
        end_date = pd.to_datetime(end_date, format='%d.%m.%Y')
    except Exception as e:
        raise ValueError(f'Invalid date format for {start_date} or {end_date}: {e}\n Indicate date in format dd.mm.yyyy')
    connection = create_connection()
    cursor = connection.cursor()
    query = """
        SELECT * FROM dam_data WHERE date >= %s AND date <= %s
    """
    cursor.execute(query, (start_date, end_date))
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=['date', 'hour', 'price', 'volume']).sort_values(by=['date', 'hour'])
    cursor.close()
    connection.close()
    return df

def table_exists():
    connection = create_connection()
    cursor = connection.cursor()
    query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_name = 'dam_data'
        )
    """
    cursor.execute(query)
    exists = cursor.fetchone()[0]
    cursor.close()
    connection.close()
    return exists
