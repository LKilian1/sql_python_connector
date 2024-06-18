import pymysql

def get_database_connect():
    try:
        connection = pymysql.connect(
            host='141.57.28.240',
            user='python',
            password='dreyertech',
            database='test',
            port=3306
        )
        print("Verbindung erfolgreich")
        
        with connection.cursor() as cursor:
            # Tabellen in der Datenbank auflisten
            cursor.execute("SHOW TABLES;")
            tables = cursor.fetchall()
            print("Tabellen in der Datenbank:")
            for table in tables:
               print(table[0])

            # # Inhalte einer bestimmten Tabelle anzeigen (z.B. 'your_table_name')
            # table_name = 'temp_messung_z'  # Ändere dies auf den Namen der gewünschten Tabelle
            # cursor.execute(f"SELECT * FROM {table_name};")
            # rows = cursor.fetchall()
            
            # # Spaltennamen abrufen
            # cursor.execute(f"SHOW COLUMNS FROM {table_name};")
            # columns = cursor.fetchall()
            # column_names = [column[0] for column in columns]
            
            # # Ergebnisse ausgeben
            # print(f"Inhalte der Tabelle '{table_name}':")
            # print(column_names)
            # for row in rows:
            #     print(row)    

        connection.close()

    except pymysql.MySQLError as e:
        print(f"Verbindungsfehler: {e}")

if __name__ == "__main__":
    get_database_connect()
