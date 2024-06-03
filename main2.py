import pymysql
import pathlib
import csv

# Verbindung zu MariaDB-Datenbank aufbauen
def connect_to_db():
    try:
        connection = pymysql.connect(
            host='141.57.28.240',
            user='python',
            password='dreyertech',
            database='test',
            port = 3306
        )
        return connection
    except pymysql.MySQLError as e:
        print(f"Error connecting to the database: {e}")
        return None

# Maximalen Wert der 'messreihe_id' ermitteln und inkrementieren
def get_next_messreihe_id(cursor):
    cursor.execute("SELECT MAX(messreihe_id) FROM messreihe;")
    result = cursor.fetchone()
    max_id = result[0] if result[0] is not None else 0
    return max_id + 1

# Tabellenstruktur basierend auf CSV-Headern erstellen
def create_table_from_csv(cursor, table_name, headers):
    columns = ', '.join([f"'{header}' FLOAT" for header in headers])
    create_table_query = f"CREATE TABLE IF NOT EXISTS '{table_name}'({columns});"
    cursor.execute(create_table_query)

# Daten in die Tabelle einfügen
def insert_data_into_table(cursor, table_name, data):
    placeholders = ', '.join(['%s'] * len(data))
    columns = ', '.join([f"'{col}" for col in data.keys()])
    insert_query = f"INSERT INTO '{table_name}' ({columns}) VALUES ({placeholders})"
    cursor.execute(insert_query, list(data.values()))

# CSV Dateien verarbeiten
def process_csv_files():
    dir = '/home/kilian/Seafile/RELAXO/MESSUNGEN/Impedanzspektroskopie/September2023'

    # Verbindung zur Datenbank herstellen
    connection = connect_to_db()
    if connection is None:
        print("Failed to connect to the database.")
        return

    cursor = connection.cursor()

    # Maximalen Wert der 'messreihe_id' ermitteln und inkrementieren
    messreihe_id = get_next_messreihe_id(cursor)

    for path in pathlib.Path(dir).rglob('*.csv'):
        p = str(path).split('/')
        if "TEMP" in p[-2]:
            with open(path) as csvfile:
                # Feste Variablen
                messgeraet_id = 1
                # Aus Dateiname
                stab_id = p[-1][4:5]
                dauer = 'unknown'
                if "OPEN" in p[-3]:
                    s = p[-1].split('_')
                    dauer = s[-1]
                # Aus Datei
                row = csvfile.readline().split(';')
                timestamp = row[0]
                temp = row[1]
                volt = p[-2].split('_')[2]
                # CSV Header und Daten lesen
                spamreader = csv.reader(csvfile, delimiter=';')
                headers = next(spamreader)
                # Tabellenname aus Dateiname ableiten
                table_name = f"{p[-2]}_{stab_id}_{volt}"
                # Tabelle erstellen
                create_table_from_csv(cursor, table_name, headers)

                # Daten zeilenweise einfügen
                for row in spamreader:
                    data = {'messreihe_id': messreihe_id}
                    data = {headers[i]: float(row[i]) for i in range(len(headers))}
                    insert_data_into_table(cursor, table_name, data)
                    print(data)

    # Änderungen speichern und Verbindung schließen
    connection.commit()
    cursor.close()
    connection.close()

# Hauptprogramm ausführen
if __name__ == "__main__":
    process_csv_files()
