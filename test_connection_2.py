import mysql.connector
from mysql.connector import Error

def test_connection():
    try:
        connection = mysql.connector.connect(
            host='141.57.28.240',
            user='root',
            password='dreyertech',
            database='relaxo',
            port=61000
        )
        if connection.is_connected():
            print("Verbindung erfolgreich!")
            connection.close()
    except Error as e:
        print(f"Verbindungsfehler: {e}")

if __name__ == "__main__":
    test_connection()
