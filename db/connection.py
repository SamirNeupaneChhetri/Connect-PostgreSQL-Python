import os
from dotenv import load_dotenv
import psycopg2
from psycopg2 import OperationalError

# Load environment variables from the .env file
load_dotenv()

class PostgresDB:
    def __init__(self) -> None:
        self.host = os.getenv('DATABASE_HOST')
        self.database = os.getenv('DATABASE_NAME')
        self.user = os.getenv('DATABASE_USER')
        self.password = os.getenv('DATABASE_PASSWORD')
        self.port = os.getenv('DATABASE_PORT', '5432')  # Default port if not provided
        self.connection = None
        self.cursor = None
    
    def connect(self) -> None:
        """Establish a connection to the PostgreSQL database."""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                port=self.port
            )
            self.cursor = self.connection.cursor()
            print("Connection to PostgreSQL DB successful")
        except OperationalError as e:
            print(f"The error '{e}' occurred")
            self.connection = None
    
    def create_table(self) -> None:
        """Create a table and insert sample data."""
        if self.connection:
            try:
                # Create table
                self.cursor.execute('''
                    CREATE TABLE IF NOT EXISTS emp (
                        id INT PRIMARY KEY,
                        name VARCHAR(50) NOT NULL,
                        age INT NOT NULL,
                        gender CHAR
                    );
                ''')
                
                # Insert sample data
                self.cursor.execute('''
                    INSERT INTO emp (id, name, age, gender) VALUES
                    (1, 'Samir', 20, 'M'),
                    (2, 'Hari', 19, 'M'),
                    (3, 'Riya', 22, 'F'),
                    (4, 'Sandhaya', 21, 'F')
                    ON CONFLICT (id) DO NOTHING;  
                ''')
                
                self.connection.commit()
                print('Table created and sample data inserted successfully')
            except OperationalError as e:
                print(f"Error creating table: {e}")
    
    def close(self) -> None:
        """Close the database connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            print("PostgreSQL connection closed")

