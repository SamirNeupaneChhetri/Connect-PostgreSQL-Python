
# app.py
from db.connection import PostgresDB

class Main:
    def __init__(self) -> None:
        print("Program Starting.")
        db = PostgresDB()
        db.connect()
        db.create_table()
        db.close()
        print("Program Ending.")
        return None
    
if __name__ == "__main__":
    app = Main()

