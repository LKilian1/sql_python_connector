import pymysql
import pandas as pd
import pathlib

class MariaDBImporter:
    def __init__(self, host, user, password, database, port):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.connection = None

    def connect(self):
        try:
            self.connection = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                port=self.port
            )
            print("Connection to MariaDB successful")
        except pymysql.MySQLError as e:
            print(f"Error connecting to MariaDB: {e}")
            self.connection = None

    def import_csv_to_table(self, csv_file, table_name):
        if self.connection is None:
            print("No connection to the database.")
            return
        
        try:
            # Read CSV file
            data = pd.read_csv(csv_file)

            # Create a cursor object
            cursor = self.connection.cursor()

            # Ensure all columns are separated by commas
            cols = ",".join([str(i) for i in data.columns.tolist()])
            placeholders = ",".join(["%s"] * len(data.columns))

            for i, row in data.iterrows():
                sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
                print(sql)  # Debug: Print SQL statement
                print(tuple(row))  # Debug: Print row values
                cursor.execute(sql, tuple(row))

            # Commit the transaction
            self.connection.commit()
            print(f"Data from {csv_file} has been imported into {table_name}")
        except Exception as e:
            print(f"Failed to import data: {e}")

    def update_messreihe(self, messreihe_id, mess_id_t, mess_id_z, mess_id_i_u):
        try:
            cursor = self.connection.cursor()
            sql = f"""
            UPDATE messreihe SET
            mess_id_t = %s,
            mess_id_z = %s,
            mess_id_i_u = %s
            WHERE messreihe_id = %s;
            """
            cursor.execute(sql, (mess_id_t, mess_id_z, mess_id_i_u, messreihe_id))
            self.connection.commit()
            print(f"Updated messreihe {messreihe_id} successfully.")
        except Exception as e:
            print(f"Failed to update messreihe: {e}")

    def close_connection(self):
        if self.connection:
            self.connection.close()
            print("Connection to MariaDB closed")
    

    def close_connection(self):
        if self.connection:
            self.connection.close()
            print("Connection to MariaDB closed")


def main():
    # MariaDB Importer initialisieren
    importer = MariaDBImporter(
        host="141.57.28.240",
        user="python",
        password="dreyertech",
        database="test",
        port=3306
    )

    # Verbindung zur Datenbank herstellen
    importer.connect()

    # Verzeichnis, in dem sich die zu importierenden CSV-Dateien befinden
    dir = '/home/kilian/Documents/Python_Project/tables'
    
    # Importieren der generierten CSV-Dateien in die entsprechenden Tabellen
    for path in pathlib.Path(dir).rglob('*.csv'):
        print(path)
        exit(0)
        if 'stab' in str(path):
            if '_t' in str(path):
                importer.import_csv_to_table(str(path), "messung_t")
            elif '_z' in str(path):
                importer.import_csv_to_table(str(path), "messung_z")
            elif '_i_u' in str(path):
                importer.import_csv_to_table(str(path), "messung_i_u")

    # Verbindung zur Datenbank schließen
    importer.close_connection()

if __name__ == "__main__":
    main()
