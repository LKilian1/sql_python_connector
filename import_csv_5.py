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
            data = pd.read_csv(csv_file)
            cursor = self.connection.cursor()
            cols = ",".join([str(i) for i in data.columns.tolist()])
            placeholders = ",".join(["%s"] * len(data.columns))
            sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
            for i, row in data.iterrows():
                cursor.execute(sql, tuple(row))
            self.connection.commit()
            print(f"Data from {csv_file} has been imported into {table_name}")
        except Exception as e:
            print(f"Failed to import data: {e}")

    def close_connection(self):
        if self.connection:
            self.connection.close()
            print("Connection to MariaDB closed")

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

    def extract_ids_and_import_init_files(self, init_files):
    # Wörterbuch zur Speicherung der letzten IDs jeder Kategorie
        last_ids = {'mess_id_t': None, 'mess_id_z': None, 'mess_id_i_u': None}
    
        for file in init_files:
            data = pd.read_csv(file)
            if '_t' in file:
                last_ids['mess_id_t'] = data['mess_id_t'].iloc[-1]  # Annahme, dass die ID in der Spalte 'mess_id_t' steht
                table_name = 'messung_t'
            elif '_z' in file:
                last_ids['mess_id_z'] = data['mess_id_z'].iloc[-1]
                table_name = 'messung_z'
            elif '_i_u' in file:
                last_ids['mess_id_i_u'] = data['mess_id_i_u'].iloc[-1]
                table_name = 'messung_i_u'
        
            self.import_csv_to_table(file, table_name)

        return last_ids    

def main():
    importer = MariaDBImporter(host="141.57.28.240", user="python", password="dreyertech", database="test", port=3306)
    importer.connect()

    messreihe_id = 6

    dir = '/home/kilian/Documents/Python_Project/tables'
    messreihe_files = []
    init_files = []
    data_files = []

    for path in pathlib.Path(dir).rglob('*.csv'):
        file_name = str(path)
        if 'messreihe' in file_name:
            messreihe_files.append(file_name)
        elif 'init' in file_name:
            init_files.append(file_name)
        else:
            data_files.append(file_name)
    # print(data_files)
    # exit(0)

    # Import messreihe files
    for file in messreihe_files:
        importer.import_csv_to_table(file, "messreihe")

        # Import init files and extract IDs
    last_ids = importer.extract_ids_and_import_init_files(init_files)
        # print(last_ids)
        # Update messreihe with the last seen IDs
    importer.update_messreihe(messreihe_id, last_ids['mess_id_t'], last_ids['mess_id_z'], last_ids['mess_id_i_u'])
    messreihe_id += 1

    exit(0)
    # Import data files
    for file in data_files:
        table_name = 'messung_t' if '_t' in file else 'messung_z' if '_z' in file else 'messung_i_u'
        importer.import_csv_to_table(file, table_name)

    importer.close_connection()

if __name__ == "__main__":
    main()
