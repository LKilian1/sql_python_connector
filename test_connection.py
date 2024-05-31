import pymysql

def test_connection():
    try:
        connection = pymysql.connect(
            host='141.57.28.240',
            user='python',
            password='dreyertech',
            database='test',
            port=3306
        )
        print("Verbindung erfolgreich!")
        connection.close()
    except pymysql.MySQLError as e:
        print(f"Verbindungsfehler: {e}")

if __name__ == "__main__":
    test_connection()
