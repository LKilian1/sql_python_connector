import pandas as pd

# Pfad zur CSV-Datei
file_path = '/home/kilian/Seafile/RELAXO/MESSUNGEN/Impedanzspektroskopie/September2023/TEMP_025_0V/Stab04_1_1000_1000_09_12_2023__16_39_40.csv'

# CSV-Datei einlesen und die erste Zeile überspringen
data = pd.read_csv(file_path, skiprows=1)

# Die erste Zeile einlesen, um den timestamp zu extrahieren
with open(file_path, 'r') as file:
    first_line = file.readline().strip()

# Den timestamp extrahieren und in das gewünschte Format konvertieren
timestamp = first_line.split(';')[0].replace('-', '').replace(' ', '').replace(':', '')[:12]
initial_timestamp = int(timestamp)

# Eine neue Spalte 'timestamp' erstellen und die Werte setzen
timestamps = [initial_timestamp + i * 10 for i in range(len(data))]
data.insert(0, 'timestamp', timestamps)

# Die Daten in eine neue CSV-Datei speichern
output_path = '/home/kilian/Documents/Python_Project/Stab04_1_1000_1000_09_12_2023__16_39_40_modified_data2.csv'
data.to_csv(output_path, index=False)

print(f'Die modifizierte Datei wurde erfolgreich unter {output_path} gespeichert.')
