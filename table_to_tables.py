import pandas as pd

# Pfad zur CSV-Datei
file_path = '/home/kilian/Seafile/RELAXO/MESSUNGEN/Impedanzspektroskopie/September2023/TEMP_025_0V//Stab04_1_1000_1000_09_12_2023__16_39_40.csv'

# Extrahieren der relevanten IDs aus dem Dateipfad
messreihe_id = 1  # Ersetzen Sie dies durch die tatsächliche Logik zur Inkrementierung
stab_id = 4  # Extrahieren Sie dies aus dem Dateinamen
mess_id_t = 1  # Diese IDs sind konstant für die CSV-Datei
mess_id_z = 6
mess_id_i_u = 1

# Die erste Zeile einlesen, um timestamp, temperatur und spannung zu extrahieren
with open(file_path, 'r') as file:
    first_line = file.readline().strip()

# Den timestamp, temperatur und spannung extrahieren
timestamp, temperatur, spannung = first_line.split(';')[:3]
timestamp = timestamp.replace('-', '').replace(' ', '').replace(':', '')[:12]
initial_timestamp = int(timestamp)
temperatur = float(temperatur)
spannung = float(spannung)

# CSV-Datei einlesen und die erste Zeile überspringen
data = pd.read_csv(file_path, skiprows=1)

# Eine neue Spalte 'timestamp' erstellen und die Werte setzen
timestamps = [initial_timestamp + i * 10 for i in range(len(data))]
data.insert(0, 'timestamp', timestamps)

# Konstanten Spalten 'temperatur' und 'spannung' hinzufügen
data['temperatur'] = temperatur
data['spannung'] = spannung

# Zusätzliche konstante Spalten hinzufügen
data['stab_id'] = stab_id
data['messreihe_id'] = messreihe_id
data['deleted'] = False  # Beispielwert, anpassen nach Bedarf

# Tabelle 1 erstellen
tabelle1_columns = ['timestamp', 'mess_id_t', 'stab_id', 'messgeraet_id', 'messreihe_id', 'temperatur', 'deleted']
data['mess_id_t'] = mess_id_t
data['messgeraet_id'] = 3
tabelle1 = data[tabelle1_columns]
tabelle1 = tabelle1.rename(columns={'messgeraet_id': 'messgeraet_id'})

# Fehlende Spalten für Tabelle 2 hinzufügen
columns_needed_tabelle2 = ['z', 'frequency', 'phase', 'flags', 'triggers', 'param0', 'param1', 'drive', 'bias', 'absz', 'abszpwr', 'abszstddev', 'bandwidth', 'biaspwr', 'biasstddev', 'drivepwr', 'drivestddev', 'frequencypwr', 'frequencystddev', 'grid', 'image', 'imagzpwr', 'imagzstddev', 'param0pwr', 'param0stddev', 'phasez', 'phasezpwr', 'phasezstddev', 'realz', 'realzpwr', 'realzstddev', 'settling', 'tc', 'tcmeas', 'count']
for col in columns_needed_tabelle2:
    if col not in data.columns:
        data[col] = None

# Tabelle 2 erstellen
tabelle2_columns = ['timestamp', 'mess_id_z', 'stab_id', 'messgeraet_id', 'messreihe_id', 'z', 'frequency', 'phase', 'flags', 'triggers', 'param0', 'param1', 'drive', 'bias', 'absz', 'abszpwr', 'abszstddev', 'bandwidth', 'biaspwr', 'biasstddev', 'drivepwr', 'drivestddev', 'frequencypwr', 'frequencystddev', 'grid', 'image', 'imagzpwr', 'imagzstddev', 'param0pwr', 'param0stddev', 'phasez', 'phasezpwr', 'phasezstddev', 'realz', 'realzpwr', 'realzstddev', 'settling', 'tc', 'tcmeas', 'count', 'deleted']
data['mess_id_z'] = mess_id_z
data['messgeraet_id'] = 1
tabelle2 = data[tabelle2_columns]
tabelle2 = tabelle2.rename(columns={'messgeraet_id': 'messgeraet_id'})

# Fehlende Spalten für Tabelle 3 hinzufügen
columns_needed_tabelle3 = ['i', 'u']
for col in columns_needed_tabelle3:
    if col not in data.columns:
        data[col] = None

# Tabelle 3 erstellen
tabelle3_columns = ['timestamp', 'mess_id_i_u', 'stab_id', 'messgeraet_id', 'messreihe_id', 'i', 'u', 'deleted']
data['mess_id_i_u'] = mess_id_i_u
data['messgeraet_id'] = 2
tabelle3 = data[tabelle3_columns]
tabelle3 = tabelle3.rename(columns={'messgeraet_id': 'messgeraet_id'})

# Tabellen als CSV-Dateien speichern
tabelle1.to_csv('/home/kilian/Documents/Python_Project/tabelle1.csv', index=False)
tabelle2.to_csv('/home/kilian/Documents/Python_Project/tabelle2.csv', index=False)
tabelle3.to_csv('/home/kilian/Documents/Python_Project/tabelle3.csv', index=False)

print('Die Tabellen wurden erfolgreich erstellt und gespeichert.')
