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

    def clean_data(self, data):
        # Entfernen von nicht benötigten Spalten oder Bereinigen der Spaltennamen
        data.columns = data.columns.str.strip()
        return data

    def validate_columns(self, data, expected_columns):
        data_columns = set(data.columns)
        missing_columns = set(expected_columns) - data_columns
        extra_columns = data_columns - set(expected_columns)
        if missing_columns:
            print(f"Missing columns in CSV: {missing_columns}")
        if extra_columns:
            print(f"Extra columns in CSV: {extra_columns}")
        return not missing_columns and not extra_columns
    
    def convert_nan_to_none(self, data):
        # NaN-Werte in None konvertieren
        return data.applymap(lambda x: None if pd.isna(x) else x)

    def import_csv_to_table(self, csv_file, table_name, expected_columns):
        if self.connection is None:
            print("No connection to the database.")
            return
        
        try:
            data = pd.read_csv(csv_file)
            data = self.clean_data(data)

            if not self.validate_columns(data, expected_columns):
                print(f"Skipping {csv_file} due to column mismatch.")
                return
            
            # Nan-Werte in None konvertieren
            # NaN-Werte in None konvertieren
            data = self.convert_nan_to_none(data)

            # Debug-Ausgabe nach der Konvertierung
            print(f"Data after NaN to None conversion: {data.head()}")

            cursor = self.connection.cursor()

            # Debug-Ausgabe der Spaltennamen
            print(f"Importing {csv_file} into {table_name}")
            print(f"CSV Columns: {list(data.columns)}")
            print(f"Expected Columns: {expected_columns}")

            for _, row in data.iterrows():
                columns = ', '.join(expected_columns)
                placeholders = ', '.join(['%s'] * len(expected_columns))
                sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                cursor.execute(sql, tuple(row[col] for col in expected_columns))
            self.connection.commit()
            print(f"Imported {csv_file} into {table_name}")
        except Exception as e:
            print(f"Error importing {csv_file} into {table_name}: {e}")

    def import_data(self, directory):
        files = {'mr': {}, 'init': {}, 'data': {}}

        # Dateien den Listen zuordnen
        for path in pathlib.Path(directory).rglob('*.csv'):
            file_name = str(path)
            messreihe_id = self.extract_messreihe_id(file_name)
            if 'mr' in file_name:
                if messreihe_id not in files['mr']:
                    files['mr'][messreihe_id] = []
                files['mr'][messreihe_id].append(file_name)
            elif 'init' in file_name:
                if messreihe_id not in files['init']:
                    files['init'][messreihe_id] = []
                files['init'][messreihe_id].append(file_name)
            elif 'data' in file_name:
                if messreihe_id not in files['data']:
                    files['data'][messreihe_id] = []
                files['data'][messreihe_id].append(file_name)

        # Sortieren nach messreihe_id
        sorted_messreihe_ids = sorted(files['mr'].keys())

        # Import in der richtigen Reihenfolge
        for messreihe_id in sorted_messreihe_ids:
            # Import der init-messreihe Dateien
            for file in files['mr'][messreihe_id]:
                self.import_csv_to_table(file, 'messreihe', ['messreihe_id', 'stab_id', 'bemerkung', 'mess_id_i_u', 'mess_id_t', 'mess_id_z', 'deleted'])

            # Import der init-messung Dateien
            for file in files['init'][messreihe_id]:
                if '_t_' in file:
                    self.import_csv_to_table(file, 'messung_t', ['timestamp', 'mess_id_t', 'stab_id', 'messgeraet_id', 'messreihe_id', 'temperatur', 'deleted'])
                elif '_z_' in file:
                    self.import_csv_to_table(file, 'messung_z', ['timestamp', 'mess_id_z', 'stab_id', 'messgeraet_id', 'messreihe_id', 'deleted'])
                elif '_i_u_' in file:
                    self.import_csv_to_table(file, 'messung_i_u', ['timestamp', 'mess_id_i_u', 'stab_id', 'messgeraet_id', 'messreihe_id', 'i', 'u', 'deleted'])

            # Aktualisieren der Tabelle 'messreihe'
            cursor = self.connection.cursor()
            cursor.execute("""
            UPDATE messreihe
            SET mess_id_i_u = (SELECT mess_id_i_u FROM messung_i_u WHERE messreihe_id = %s LIMIT 1),
                mess_id_t = (SELECT mess_id_t FROM messung_t WHERE messreihe_id = %s LIMIT 1),
                mess_id_z = (SELECT mess_id_z FROM messung_z WHERE messreihe_id = %s LIMIT 1)
            WHERE messreihe_id = %s
            """, (messreihe_id, messreihe_id, messreihe_id, messreihe_id))
            self.connection.commit()
            print(f"Updated messreihe for messreihe_id {messreihe_id}")

            # Import der data Dateien
            for file in files['data'][messreihe_id]:
                if '_t_' in file:
                    self.import_csv_to_table(file, 'messung_t', ['timestamp', 'mess_id_t', 'stab_id', 'messgeraet_id', 'messreihe_id', 'temperatur', 'deleted'])
                elif '_z_' in file:
                    self.import_csv_to_table(file, 'messung_z', ['timestamp', 'mess_id_z', 'stab_id', 'messgeraet_id', 'messreihe_id', 'absz', 'abszpwr', 'abszstddev', 'frequency', 'frequencypwr', 'frequencystddev', 'imagz', 'imagzpwr', 'imagzstddev', 'param0', 'param0pwr', 'param0stddev', 'param1', 'param1pwr', 'param1stddev', 'phasez', 'phasezpwr', 'phasezstddev', 'realz', 'realzpwr', 'deleted'])
                elif '_i_u_' in file:
                    self.import_csv_to_table(file, 'messung_i_u', ['timestamp', 'mess_id_i_u', 'stab_id', 'messgeraet_id', 'messreihe_id', 'i', 'u', 'deleted'])

    def extract_messreihe_id(self, file_name):
        return int(file_name.split('_')[-1].split('.')[0])

# Beispielhafte Nutzung der Klasse
if __name__ == "__main__":
    importer = MariaDBImporter(host='141.57.28.240', user='python', password='dreyertech', database='relaxo', port=3306)
    importer.connect()
    importer.import_data('//home/kilian/Documents/Python_Project/tables')

