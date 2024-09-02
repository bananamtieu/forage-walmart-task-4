import pandas as pd
import sqlite3

# Function to load data from the first CSV file
def load_first_csv(file_path, conn):
    # Load the CSV file into a pandas DataFrame
    data = pd.read_csv(file_path)
    
    # Create a table for the first CSV data if it doesn't exist
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS shipments (
        id INTEGER PRIMARY KEY,
        origin_warehouse TEXT,
        destination_store TEXT,
        product TEXT,
        on_time BOOLEAN,
        product_quantity INTEGER,
        driver_identifier TEXT
    )
    ''')
    
    # Insert data into the database
    for index, row in data.iterrows():
        cursor.execute('''
        INSERT INTO shipments (origin_warehouse, destination_store, product, on_time, product_quantity, driver_identifier)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            row['origin_warehouse'],
            row['destination_store'],
            row['product'],
            row['on_time'],
            row['product_quantity'],
            row['driver_identifier']
        ))

# Function to load data from the second CSV file
def load_second_csv(file_path, conn):
    # Load the CSV file into a pandas DataFrame
    data = pd.read_csv(file_path)
    
    # Create a table for the second CSV data if it doesn't exist
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS additional_shipments (
        id INTEGER PRIMARY KEY,
        shipment_identifier TEXT,
        product TEXT,
        on_time BOOLEAN
    )
    ''')
    
    # Insert data into the database
    for index, row in data.iterrows():
        cursor.execute('''
        INSERT INTO additional_shipments (shipment_identifier, product, on_time)
        VALUES (?, ?, ?)
        ''', (
            row['shipment_identifier'],
            row['product'],
            row['on_time']
        ))

# Function to load data from the third CSV file
def load_third_csv(file_path, conn):
    # Load the CSV file into a pandas DataFrame
    data = pd.read_csv(file_path)
    
    # Create a table for the third CSV data if it doesn't exist
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS shipment_details (
        id INTEGER PRIMARY KEY,
        shipment_identifier TEXT,
        origin_warehouse TEXT,
        destination_store TEXT,
        driver_identifier TEXT
    )
    ''')
    
    # Insert data into the database
    for index, row in data.iterrows():
        cursor.execute('''
        INSERT INTO shipment_details (shipment_identifier, origin_warehouse, destination_store, driver_identifier)
        VALUES (?, ?, ?, ?)
        ''', (
            row['shipment_identifier'],
            row['origin_warehouse'],
            row['destination_store'],
            row['driver_identifier']
        ))

# Main function to execute the loading process
def main():
    # Connect to (or create) the SQLite database
    conn = sqlite3.connect('shipment_database.db')
    
    # Load data from the first CSV file
    load_first_csv('data/shipping_data_0.csv', conn)
    
    # Load data from the second CSV file
    load_second_csv('data/shipping_data_1.csv', conn)
    
    # Load data from the third CSV file
    load_third_csv('data/shipping_data_2.csv', conn)
    
    # Commit changes and close the connection
    conn.commit()
    conn.close()
    
    print("Data from all files has been successfully inserted into the SQLite database.")

# Execute the main function
if __name__ == "__main__":
    main()
